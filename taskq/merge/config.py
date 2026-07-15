"""Resolution and validation for standalone merge specifications."""

import hashlib
import json
import shlex
from pathlib import Path

from ..backends.base import BackendError
from ..common import user_cache_dir
from ..git import (
    branch_worktrees,
    common_dir,
    ensure_local_branch,
    git,
    local_branch_ref,
    repository_root,
    resolve_commit,
)


SPEC_VERSION = 1
DEFAULT_COMMAND = ['codex', 'exec', '--ephemeral', '{}']
DEFAULT_CONFLICT_PROMPT = """Resolve the in-progress Git cherry-pick conflict.

Repository: $repo_root
Destination branch: $target_branch
Destination head used for staging: $target_head
Taskq parent job: $job_id
Immutable change commit: $change_head
Original command: $command

Work only in the current taskq staging worktree. Inspect every conflict, preserve
the intent of both the destination and the queued change, run relevant focused
checks when practical, stage every resolution, and complete the existing
cherry-pick with `git cherry-pick --continue`. Do not abort the cherry-pick,
switch branches, rewrite unrelated history, or leave an unclean worktree.
"""


def _plain(value):
    try:
        return json.loads(json.dumps(value))
    except (TypeError, ValueError) as error:
        raise BackendError('merge configuration must be JSON serializable') from error


def parse_command(command):
    if isinstance(command, str):
        try:
            command = shlex.split(command)
        except ValueError as error:
            raise BackendError('invalid merge resolver command: {}'.format(error))
    else:
        command = list(command or [])
    if not command or any(not isinstance(value, str) for value in command):
        raise BackendError('merge resolver command must contain string arguments')
    if command.count('{}') != 1:
        raise BackendError(
            'merge resolver command must contain exactly one standalone {} token')
    if any('\0' in value for value in command):
        raise BackendError('merge resolver command cannot contain NUL bytes')
    return command


def repository_key(git_common_dir):
    return hashlib.sha256(
        str(Path(git_common_dir).resolve()).encode('utf-8')).hexdigest()[:24]


def lane_key(git_common_dir, target_ref):
    identity = '{}\0{}'.format(Path(git_common_dir).resolve(), target_ref)
    return hashlib.sha256(identity.encode('utf-8')).hexdigest()[:24]


def state_root(git_common_dir, environ=None):
    return user_cache_dir(environ) / 'merge' / repository_key(git_common_dir)


def _resolver(config):
    merge = dict(config.get('merge') or {})
    explore = dict(config.get('explore') or {})
    explore_merge = dict(explore.get('merge') or {})
    command = merge.get('command')
    if command is None:
        command = explore_merge.get('command', explore.get('command', DEFAULT_COMMAND))
    prompt = merge.get('conflict_prompt')
    if prompt is None:
        prompt = explore_merge.get('rebase_prompt', DEFAULT_CONFLICT_PROMPT)
    if not isinstance(prompt, str) or not prompt.strip():
        raise BackendError('merge conflict prompt must be non-empty text')
    timeout = merge.get('timeout')
    if timeout is None:
        timeout = explore_merge.get('timeout', explore.get('timeout', 1800))
    try:
        timeout = float(timeout)
    except (TypeError, ValueError) as error:
        raise BackendError('merge resolver timeout must be a number') from error
    if timeout < 0:
        raise BackendError('merge resolver timeout cannot be negative')
    return {
        'command': parse_command(command),
        'conflict_prompt': prompt.strip(),
        'timeout': timeout,
    }


def _reject_checked_out_target(cwd, target_branch, target_ref):
    checkouts = branch_worktrees(cwd, target_ref)
    if not checkouts:
        return
    paths = ', '.join(item['worktree'] for item in checkouts)
    raise BackendError(
        'merge destination branch {!r} is checked out in {}'.format(
            target_branch, paths))


def build_merge_spec(config, cwd, branch=None, create=True):
    """Capture a JSON-safe destination and resolver specification.

    Committed submissions atomically create a missing destination from the
    source HEAD captured here. Validation-only callers can pass
    ``create=False`` to describe that hypothetical branch without mutating the
    repository. The destination may move later; the controller detects that
    and rebuilds its staging chain.
    """
    cwd = str(Path(cwd).expanduser().resolve())
    root = repository_root(cwd)
    common = common_dir(root)
    if branch is None:
        raise BackendError('merge destination branch must be specified')
    source_head = resolve_commit(cwd)
    target_branch, target_ref = local_branch_ref(root, branch)
    _reject_checked_out_target(root, target_branch, target_ref)
    if create:
        target_branch, target_ref, target_head = ensure_local_branch(
            root,
            target_branch,
            source_head,
            message='taskq create merge destination',
        )
    else:
        exists = git(
            root, 'show-ref', '--verify', '--quiet', target_ref, check=False)
        target_head = (
            resolve_commit(root, target_ref)
            if exists.returncode == 0 else source_head
        )
    # Narrow the race with a checkout that starts while a missing branch is
    # being created. Landing repeats this invariant under taskq's ref lock.
    _reject_checked_out_target(root, target_branch, target_ref)
    repo_state = state_root(common)
    lane = lane_key(common, target_ref)
    spec = {
        'version': SPEC_VERSION,
        'requested': True,
        'repo_root': root,
        'git_common_dir': common,
        'source_cwd': cwd,
        'source_head': source_head,
        'target_branch': target_branch,
        'target_ref': target_ref,
        'target_head': target_head,
        'repository_key': repository_key(common),
        'lane_id': lane,
        'state_path': str(repo_state / 'state.sqlite3'),
        'controller_lock': str(repo_state / 'controller.lock'),
        'controller_heartbeat': str(repo_state / 'controller.heartbeat'),
        'staging_ref': 'refs/taskq/merge/lanes/{}/staging'.format(lane),
        'staging_worktree': str(repo_state / 'worktrees' / lane),
        'resolver': _resolver(config),
    }
    return _plain(spec)

"""Shared, policy-free Git operations used by taskq workflows.

The helpers in this module deliberately keep workflow policy out of Git
plumbing.  In particular, callers decide whether a dirty checkout should
block an operation and which taskq-owned refs/worktrees may be removed.
"""

import os
import shutil
import subprocess
from pathlib import Path

from .backends.base import BackendError


def git(cwd, *args, check=True, input_text=None, env=None):
    """Run Git in *cwd* and return stripped stdout.

    With ``check=False`` the returned object is ``CompletedProcess`` so the
    caller can inspect both the return code and diagnostics.
    """
    command = ['git', '-C', str(cwd), *map(str, args)]
    try:
        result = subprocess.run(
            command,
            input=input_text,
            capture_output=True,
            check=False,
            text=True,
            env=env,
        )
    except FileNotFoundError as error:
        raise BackendError('git command not found') from error
    if check and result.returncode:
        message = (result.stderr or result.stdout or '').strip()
        raise BackendError(message or 'git command failed')
    return result.stdout.strip() if check else result


def repository_root(cwd):
    return str(Path(git(cwd, 'rev-parse', '--show-toplevel')).resolve())


def common_dir(cwd):
    root = Path(repository_root(cwd))
    value = Path(git(root, 'rev-parse', '--git-common-dir'))
    if not value.is_absolute():
        value = root / value
    return str(value.resolve())


def repository(cwd, require_branch=True):
    root = repository_root(cwd)
    result = git(root, 'symbolic-ref', '--quiet', '--short', 'HEAD', check=False)
    branch = result.stdout.strip() if result.returncode == 0 else None
    if require_branch and not branch:
        raise BackendError('detached HEAD has no current branch')
    return root, branch, git(root, 'rev-parse', 'HEAD')


def resolve_commit(cwd, ref='HEAD'):
    return git(cwd, 'rev-parse', '--verify', '{}^{{commit}}'.format(ref))


def local_branch(cwd, branch):
    """Validate and return ``(short_name, full_ref, head)``."""
    if not isinstance(branch, str) or not branch.strip():
        raise BackendError('target branch must be a non-empty local branch name')
    branch = branch.strip()
    if branch.startswith('refs/heads/'):
        short = branch[len('refs/heads/'):]
    else:
        short = branch
    if not short or short.startswith('-') or '..' in short:
        raise BackendError('invalid local target branch {!r}'.format(branch))
    ref = 'refs/heads/{}'.format(short)
    check = git(cwd, 'show-ref', '--verify', '--quiet', ref, check=False)
    if check.returncode:
        raise BackendError('local target branch does not exist: {}'.format(short))
    return short, ref, resolve_commit(cwd, ref)


def current_branch(cwd):
    result = git(cwd, 'symbolic-ref', '--quiet', '--short', 'HEAD', check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def is_ancestor(cwd, ancestor, descendant):
    result = git(
        cwd, 'merge-base', '--is-ancestor', ancestor, descendant, check=False)
    return result.returncode == 0


def status(cwd):
    return git(cwd, 'status', '--porcelain', '--untracked-files=normal')


def is_clean(cwd):
    return not status(cwd)


def changed_paths(cwd, base='HEAD'):
    tracked = git(cwd, 'diff', '--name-only', base)
    untracked = git(cwd, 'ls-files', '--others', '--exclude-standard')
    return [line for line in (tracked + '\n' + untracked).splitlines() if line]


def changed_paths_between(cwd, base, head):
    output = git(
        cwd, 'diff', '--name-only', '-z', '--no-ext-diff', base, head)
    return [path for path in output.split('\0') if path]


def ignored_untracked_paths(cwd):
    output = git(
        cwd, 'ls-files', '--others', '--ignored', '--exclude-standard', '-z')
    return [path for path in output.split('\0') if path]


def core_ignorecase(cwd):
    """Return Git's case-folding policy for paths in this worktree."""
    result = git(
        cwd, 'config', '--bool', '--get', 'core.ignorecase', check=False)
    return result.returncode == 0 and result.stdout.strip().lower() == 'true'


def path_collisions(left, right, ignore_case=False):
    """Return paths colliding by equality or file/directory ancestry."""
    def indexed(paths):
        values = {}
        for path in paths:
            path = path.strip('/')
            key = path.casefold() if ignore_case else path
            values.setdefault(key, set()).add(path)
        return values

    def ancestors(path):
        return (path[:index] for index, value in enumerate(path) if value == '/')

    left = indexed(left)
    right = indexed(right)
    collisions = set()
    for key in left.keys() & right.keys():
        collisions.update(left[key])
        collisions.update(right[key])
    for key, paths in left.items():
        for ancestor in ancestors(key):
            if ancestor in right:
                collisions.update(paths)
                collisions.update(right[ancestor])
    for key, paths in right.items():
        for ancestor in ancestors(key):
            if ancestor in left:
                collisions.update(paths)
                collisions.update(left[ancestor])
    return sorted(path for path in collisions if path)


def snapshot(cwd, message):
    """Commit the current tracked and non-ignored untracked state."""
    if is_clean(cwd):
        return resolve_commit(cwd), False
    git(cwd, 'add', '-A')
    git(
        cwd, '-c', 'user.name=taskq', '-c', 'user.email=taskq@localhost',
        'commit', '--no-gpg-sign', '--no-verify', '-m', message)
    return resolve_commit(cwd), True


def synthetic_change_commit(cwd, source_base, message):
    """Create one immutable ``source_base -> final tree`` change commit.

    The job may have made commits of its own.  Only its final tree is used, so
    its internal topology and unrelated source history cannot be imported into
    the destination lane.  ``git add -A`` includes tracked changes and
    non-ignored untracked files while leaving ignored files out.
    """
    source_base = resolve_commit(cwd, source_base)
    git(cwd, 'add', '-A')
    result_tree = git(cwd, 'write-tree')
    source_tree = git(cwd, 'rev-parse', '{}^{{tree}}'.format(source_base))
    if result_tree == source_tree:
        return None, result_tree
    commit = git(
        cwd,
        '-c', 'user.name=taskq', '-c', 'user.email=taskq@localhost',
        'commit-tree', result_tree, '-p', source_base, '-m', message,
    )
    return commit, result_tree


def diff(cwd, base, head='HEAD', limit=50000):
    text = git(cwd, 'diff', '--no-ext-diff', '--binary', '{}..{}'.format(base, head))
    return text if len(text) <= limit else text[:limit] + '\n[diff truncated]\n'


def update_ref(cwd, ref, new, old=None, message=None):
    args = ['update-ref']
    if message:
        args += ['-m', message]
    args += [ref, new]
    if old is not None:
        args.append(old)
    git(cwd, *args)
    return resolve_commit(cwd, ref)


def delete_ref(cwd, ref, old=None):
    args = ['update-ref', '-d', ref]
    if old is not None:
        args.append(old)
    result = git(cwd, *args, check=False)
    return result.returncode == 0


def worktrees(cwd):
    """Return parsed ``git worktree list --porcelain`` records."""
    output = git(cwd, 'worktree', 'list', '--porcelain', '-z')
    records = []
    for raw in output.split('\0\0'):
        fields = [field for field in raw.split('\0') if field]
        if not fields:
            continue
        record = {}
        for field in fields:
            key, _, value = field.partition(' ')
            if key in {'bare', 'detached', 'locked', 'prunable'}:
                record[key] = value or True
            else:
                record[key] = value
        if record.get('worktree'):
            records.append(record)
    return records


def branch_worktrees(cwd, target_ref):
    return [item for item in worktrees(cwd) if item.get('branch') == target_ref]


def add_detached_worktree(cwd, path, start_point):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    git(cwd, 'worktree', 'add', '--detach', str(path), start_point)
    return str(path)


def remove_worktree(cwd, path, force=True):
    args = ['worktree', 'remove']
    if force:
        args.append('--force')
    args.append(str(path))
    result = git(cwd, *args, check=False)
    if result.returncode:
        shutil.rmtree(path, ignore_errors=True)
        git(cwd, 'worktree', 'prune', check=False)
        return False
    return True


def reset_hard(cwd, commit):
    git(cwd, 'reset', '--hard', commit)
    git(cwd, 'clean', '-fd')
    return resolve_commit(cwd)


def cherry_pick(cwd, commit):
    result = git(
        cwd,
        '-c', 'user.name=taskq', '-c', 'user.email=taskq@localhost',
        '-c', 'commit.gpgSign=false', '-c', 'core.hooksPath=/dev/null',
        'cherry-pick', '--no-gpg-sign', commit, check=False,
    )
    output = (result.stdout + result.stderr).strip()
    return result.returncode == 0, output


def cherry_pick_in_progress(cwd):
    path = Path(git(cwd, 'rev-parse', '--git-path', 'CHERRY_PICK_HEAD'))
    if not path.is_absolute():
        path = Path(cwd) / path
    return path.exists()


def abort_cherry_pick(cwd):
    if cherry_pick_in_progress(cwd):
        git(cwd, 'cherry-pick', '--abort', check=False)


def merge_ff(cwd, commit):
    git(
        cwd,
        '-c', 'commit.gpgSign=false', '-c', 'core.hooksPath=/dev/null',
        'merge', '--ff-only', '--no-edit', '--no-verify',
        '--no-overwrite-ignore', commit)
    return resolve_commit(cwd)


def atomic_write(path, text):
    """Atomically replace a UTF-8 text file in its existing filesystem."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name('.{}.{}.tmp'.format(path.name, os.getpid()))
    temporary.write_text(text, encoding='utf-8')
    os.replace(str(temporary), str(path))

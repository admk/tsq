import os
import shutil
import sys
from pathlib import Path

from .. import git as git_utils
from .base import BackendError


def git_output(args):
    args = list(args)
    cwd = os.getcwd()
    if len(args) >= 2 and args[0] == '-C':
        cwd = args[1]
        args = args[2:]
    return git_utils.git(cwd, *args)


def resolve_ref(ref, cwd=None):
    cwd = cwd or os.getcwd()
    try:
        root = git_utils.repository_root(cwd)
    except BackendError as e:
        raise BackendError(f'--ref requires a git repository: {e}') from e
    try:
        commit = git_utils.resolve_commit(root, ref)
    except BackendError as e:
        raise BackendError(f'could not resolve git ref {ref!r}: {e}') from e
    return root, commit


def worktree_cwd(source_cwd, git_root, git_worktree):
    source = Path(source_cwd).resolve()
    root = Path(git_root).resolve()
    worktree = Path(git_worktree)
    try:
        relative = source.relative_to(root)
    except ValueError as e:
        raise BackendError(f'cwd {source} is not inside git repo {root}') from e
    return worktree / relative


def _run_worktree_add(git_root, args, git_worktree):
    try:
        git_utils.git(git_root, 'worktree', 'add', *args)
    except BackendError as e:
        message = str(e).strip()
        if message == 'git command failed':
            message = ''
        raise BackendError(
            message or f'failed to create git worktree {git_worktree}'
        ) from e


def create_worktree(git_root, git_commit, git_worktree):
    try:
        git_utils.add_detached_worktree(
            git_root, git_worktree, git_commit)
    except BackendError as e:
        message = str(e).strip()
        if message == 'git command failed':
            message = ''
        raise BackendError(
            message or f'failed to create git worktree {git_worktree}'
        ) from e


def create_branch_worktree(
    git_root, branch, git_worktree, start_point='HEAD',
):
    """Create a campaign-owned worktree on a new local branch."""
    git_root = str(Path(git_root).resolve())
    git_worktree = str(Path(git_worktree).resolve())
    _run_worktree_add(
        git_root,
        ['-b', branch, git_worktree, start_point],
        git_worktree,
    )
    return {
        'git_root': git_root,
        'git_worktree': git_worktree,
        'git_branch': branch,
        'git_commit': git_utils.resolve_commit(git_worktree),
        'workspace_owner': 'campaign',
    }


def remove_worktree(meta, force=False):
    if meta.get('workspace_owner') in {'campaign', 'merge'} and not force:
        return
    git_worktree = meta.get('git_worktree')
    if not git_worktree:
        return
    git_root = meta.get('git_root')
    removed = False
    if git_root:
        try:
            removed = git_utils.remove_worktree(
                git_root, git_worktree, force=True)
        except BackendError as e:
            print(
                f'Warning: failed to unregister git worktree '
                f'{git_worktree}: {e}',
                file=sys.stderr,
            )
        else:
            if not removed:
                print(
                    f'Warning: failed to unregister git worktree '
                    f'{git_worktree}: git command failed',
                    file=sys.stderr,
                )
    if not removed:
        shutil.rmtree(git_worktree, ignore_errors=True)


def remove_branch_worktree(
    meta, delete_branch=False, force_branch=False,
):
    """Remove a named worktree and optionally its local branch."""
    remove_worktree(meta, force=True)
    branch = meta.get('git_branch')
    git_root = meta.get('git_root')
    if not delete_branch or not branch or not git_root:
        return
    flag = '-D' if force_branch else '-d'
    try:
        git_output(['-C', git_root, 'branch', flag, branch])
    except BackendError as e:
        raise BackendError(
            f'failed to delete git branch {branch!r}: {e}') from e


def remove_nested_worktrees(root):
    """Unregister linked worktrees nested below a disposable state root."""
    linked = []
    for current, dirs, files in os.walk(root):
        if '.git' not in files:
            continue
        linked.append(Path(current).resolve())
        dirs[:] = []
    for worktree in linked:
        try:
            branch = git_utils.current_branch(worktree)
            common = git_utils.common_dir(worktree)
            result = git_utils.git(
                common, '--git-dir', common, 'worktree', 'remove', '--force',
                str(worktree), check=False,
            )
            if result.returncode:
                message = (result.stderr or result.stdout or '').strip()
                raise BackendError(message or 'git command failed')
            if branch and branch.startswith('tq/explore/'):
                git_utils.git(
                    common, '--git-dir', common, 'branch', '-D', branch,
                    check=False)
        except BackendError as e:
            print(
                f'Warning: failed to unregister git worktree {worktree}: {e}',
                file=sys.stderr,
            )


def prepare_checkout(
    job_dir, git_ref=None, git_commit=None, git_root=None, source_cwd=None,
):
    if not git_ref and not git_commit:
        return {}, os.getcwd()
    source_cwd = source_cwd or os.getcwd()
    if git_commit:
        if not git_root:
            git_root, _ = resolve_ref(git_commit, source_cwd)
    else:
        git_root, git_commit = resolve_ref(git_ref, source_cwd)
    git_worktree = Path(job_dir) / 'worktree'
    create_worktree(git_root, git_commit, git_worktree)
    checkout_cwd = worktree_cwd(source_cwd, git_root, git_worktree)
    if not checkout_cwd.is_dir():
        meta = {
            'git_root': git_root,
            'git_worktree': str(git_worktree),
        }
        remove_worktree(meta)
        raise BackendError(
            f'cwd {source_cwd} does not exist at git commit {git_commit}')
    return {
        'git_ref': git_ref,
        'git_commit': git_commit,
        'git_root': git_root,
        'git_worktree': str(git_worktree),
        'source_cwd': source_cwd,
        'workspace_owner': 'job',
    }, str(checkout_cwd)

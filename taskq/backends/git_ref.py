import os
import shutil
import subprocess
import sys
from pathlib import Path

from .base import BackendError


def git_output(args):
    try:
        result = subprocess.run(
            ['git', *args],
            capture_output=True,
            check=True,
            text=True,
        )
    except FileNotFoundError as e:
        raise BackendError('git command not found') from e
    except subprocess.CalledProcessError as e:
        message = (e.stderr or e.stdout or '').strip()
        raise BackendError(message or 'git command failed') from e
    return result.stdout.strip()


def resolve_ref(ref, cwd=None):
    cwd = cwd or os.getcwd()
    try:
        root = git_output(['-C', cwd, 'rev-parse', '--show-toplevel'])
    except BackendError as e:
        raise BackendError(f'--ref requires a git repository: {e}') from e
    try:
        commit = git_output(
            ['-C', root, 'rev-parse', '--verify', f'{ref}^{{commit}}'])
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
        subprocess.run(
            ['git', '-C', git_root, 'worktree', 'add', *args],
            capture_output=True,
            check=True,
            text=True,
        )
    except FileNotFoundError as e:
        raise BackendError('git command not found') from e
    except subprocess.CalledProcessError as e:
        message = (e.stderr or e.stdout or '').strip()
        raise BackendError(
            message or f'failed to create git worktree {git_worktree}'
        ) from e


def create_worktree(git_root, git_commit, git_worktree):
    _run_worktree_add(
        git_root, ['--detach', str(git_worktree), git_commit], git_worktree)


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
        'git_commit': git_output(
            ['-C', git_worktree, 'rev-parse', 'HEAD']),
        'workspace_owner': 'campaign',
    }


def remove_worktree(meta, force=False):
    if meta.get('workspace_owner') == 'campaign' and not force:
        return
    git_worktree = meta.get('git_worktree')
    if not git_worktree:
        return
    git_root = meta.get('git_root')
    removed = False
    if git_root:
        try:
            subprocess.run(
                [
                    'git',
                    '-C',
                    git_root,
                    'worktree',
                    'remove',
                    '--force',
                    git_worktree,
                ],
                capture_output=True,
                check=True,
                text=True,
            )
            removed = True
        except (FileNotFoundError, subprocess.CalledProcessError) as e:
            print(
                f'Warning: failed to unregister git worktree '
                f'{git_worktree}: {e}',
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
            try:
                branch = git_output([
                    '-C', str(worktree), 'symbolic-ref', '--short', 'HEAD'])
            except BackendError:
                branch = None
            common = Path(git_output([
                '-C', str(worktree), 'rev-parse', '--git-common-dir',
            ]))
            if not common.is_absolute():
                common = (worktree / common).resolve()
            subprocess.run(
                [
                    'git', '--git-dir', str(common), 'worktree', 'remove',
                    '--force', str(worktree),
                ],
                capture_output=True,
                check=True,
                text=True,
            )
            if branch and branch.startswith('tq/explore/'):
                subprocess.run(
                    ['git', '--git-dir', str(common), 'branch', '-D', branch],
                    capture_output=True, check=False, text=True)
        except (BackendError, FileNotFoundError, subprocess.CalledProcessError) as e:
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

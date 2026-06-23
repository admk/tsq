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


def create_worktree(git_root, git_commit, git_worktree):
    try:
        subprocess.run(
            [
                'git',
                '-C',
                git_root,
                'worktree',
                'add',
                '--detach',
                str(git_worktree),
                git_commit,
            ],
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


def remove_worktree(meta):
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
    }, str(checkout_cwd)

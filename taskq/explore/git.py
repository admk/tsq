import datetime
import fnmatch
import re
import subprocess
from pathlib import Path

from ..backends.base import BackendError


def git(cwd, *args, check=True):
    try:
        result = subprocess.run(
            ['git', '-C', str(cwd), *map(str, args)],
            capture_output=True, check=check, text=True,
        )
    except FileNotFoundError as error:
        raise BackendError('git command not found') from error
    except subprocess.CalledProcessError as error:
        message = (error.stderr or error.stdout or '').strip()
        raise BackendError(message or 'git command failed') from error
    return result.stdout.strip()


def repository(cwd):
    root = repository_root(cwd)
    branch = git(root, 'symbolic-ref', '--short', 'HEAD')
    return root, branch, git(root, 'rev-parse', 'HEAD')


def repository_root(cwd):
    return git(cwd, 'rev-parse', '--show-toplevel')


def require_clean(cwd):
    status = git(cwd, 'status', '--porcelain', '--untracked-files=normal')
    if status:
        raise BackendError(
            'tq explore requires a clean working tree; commit, stash, or '
            'remove tracked and untracked changes first')


def ensure_local_exclude(cwd, pattern='.tq/explore/'):
    path = Path(git(cwd, 'rev-parse', '--git-path', 'info/exclude'))
    if not path.is_absolute():
        path = Path(cwd) / path
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = path.read_text(encoding='utf-8').splitlines() if path.exists() else []
    if pattern not in lines:
        with open(path, 'a', encoding='utf-8') as stream:
            if lines and lines[-1]:
                stream.write('\n')
            stream.write(pattern + '\n')


def changed_paths(cwd, base='HEAD'):
    tracked = git(cwd, 'diff', '--name-only', base)
    untracked = git(cwd, 'ls-files', '--others', '--exclude-standard')
    return [line for line in (tracked + '\n' + untracked).splitlines() if line]


def protected_paths(paths, patterns):
    return sorted({
        path for path in paths
        for pattern in patterns
        if fnmatch.fnmatch(path, pattern)
    })


def snapshot(cwd, message):
    if not git(cwd, 'status', '--porcelain', '--untracked-files=normal'):
        return git(cwd, 'rev-parse', 'HEAD'), False
    git(cwd, 'add', '-A')
    git(
        cwd, '-c', 'user.name=taskq', '-c', 'user.email=taskq@localhost',
        'commit', '--no-gpg-sign', '--no-verify', '-m', message)
    return git(cwd, 'rev-parse', 'HEAD'), True


def diff(cwd, base, head='HEAD', limit=50000):
    text = git(cwd, 'diff', '--no-ext-diff', '--binary', f'{base}..{head}')
    return text if len(text) <= limit else text[:limit] + '\n[diff truncated]\n'


def rebase(cwd, target):
    result = subprocess.run(
        ['git', '-C', str(cwd), 'rebase', target],
        capture_output=True, text=True,
    )
    return result.returncode == 0, (result.stdout + result.stderr).strip()


def rebase_in_progress(cwd):
    return any(Path(git(cwd, 'rev-parse', '--git-path', name)).exists()
               for name in ('rebase-merge', 'rebase-apply'))


def merge_ff(cwd, branch):
    git(cwd, 'merge', '--ff-only', branch)
    return git(cwd, 'rev-parse', 'HEAD')


def campaign_id(objective):
    slug = re.sub(r'[^a-z0-9]+', '-', objective.lower()).strip('-')[:32]
    stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')
    return f'{slug or "explore"}-{stamp}'

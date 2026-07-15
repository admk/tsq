import datetime
import fnmatch
import re
from pathlib import Path

from ..backends.base import BackendError
from ..git import (
    changed_paths,
    diff,
    git as _git,
    is_clean,
    merge_ff,
    repository,
    repository_root,
    snapshot,
)


def git(cwd, *args, check=True):
    """Run Git while preserving Explore's historical string return type."""
    result = _git(cwd, *args, check=check)
    return result if check else result.stdout.strip()


def require_clean(cwd):
    if not is_clean(cwd):
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


def protected_paths(paths, patterns):
    return sorted({
        path for path in paths
        for pattern in patterns
        if fnmatch.fnmatch(path, pattern)
    })


def rebase(cwd, target):
    result = _git(cwd, 'rebase', target, check=False)
    return result.returncode == 0, (result.stdout + result.stderr).strip()


def rebase_in_progress(cwd):
    return any(Path(git(cwd, 'rev-parse', '--git-path', name)).exists()
               for name in ('rebase-merge', 'rebase-apply'))


def campaign_id(objective):
    slug = re.sub(r'[^a-z0-9]+', '-', objective.lower()).strip('-')[:32]
    stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')
    return f'{slug or "explore"}-{stamp}'

"""Safe profile evaluator assets and immutable campaign snapshots."""

import hashlib
import os
import shutil
import stat
from pathlib import Path

from ..backends.base import BackendError
from .profiles import ASSET_MAX_BYTES, ASSET_MAX_FILES


def inventory(root, required=False):
    root = Path(root)
    if not root.exists():
        return []
    if root.is_symlink() or not root.is_dir():
        raise BackendError('asset root must be a regular directory: {}'.format(root))
    result = []
    total = 0
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root)
        if '..' in relative.parts:
            raise BackendError('asset path traversal is not allowed')
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        if stat.S_ISLNK(info.st_mode):
            raise BackendError('asset cannot be a symbolic link: {}'.format(relative))
        if not stat.S_ISREG(info.st_mode):
            raise BackendError('asset must be a regular file: {}'.format(relative))
        total += info.st_size
        if len(result) >= ASSET_MAX_FILES:
            raise BackendError('profile assets exceed {} files'.format(ASSET_MAX_FILES))
        if total > ASSET_MAX_BYTES:
            raise BackendError('profile assets exceed {} bytes'.format(ASSET_MAX_BYTES))
        result.append({
            'path': relative.as_posix(),
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'mode': stat.S_IMODE(info.st_mode),
            'size': info.st_size,
        })
    if required and not result:
        raise BackendError('asset snapshot is empty')
    return result


def copy_assets(source, target, expected=None):
    source, target = Path(source), Path(target)
    found = inventory(source)
    if expected is not None and found != expected:
        raise BackendError('profile assets changed while being snapshotted')
    if target.exists():
        shutil.rmtree(target)
    if found:
        target.mkdir(parents=True)
        for item in found:
            src, dst = source / item['path'], target / item['path']
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)
            os.chmod(dst, item['mode'])
    return found

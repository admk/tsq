"""Shared serialization and safety policy for taskq-controlled Git landings."""

import fcntl
import hashlib
from contextlib import contextmanager
from pathlib import Path

from .backends.base import BackendError
from .git import (
    changed_paths_between,
    common_dir,
    core_ignorecase,
    ignored_untracked_paths,
    is_clean,
    merge_ff,
    path_collisions,
    resolve_commit,
)


class LandingBlocked(BackendError):
    """The destination checkout has user state that taskq must preserve."""


class LandingMoved(BackendError):
    """The destination no longer matches the head used for integration."""


def target_integration_lock_path(cwd, target_ref):
    """Return one lock path for a repository and destination ref."""
    target_ref = str(target_ref)
    if not target_ref.startswith('refs/'):
        target_ref = 'refs/heads/{}'.format(target_ref)
    identity = target_ref.encode('utf-8')
    digest = hashlib.sha256(identity).hexdigest()[:24]
    return Path(common_dir(cwd)) / 'taskq-locks' / 'landing-{}.lock'.format(
        digest)


@contextmanager
def target_integration_lock(cwd, target_ref):
    """Serialize taskq landing preflight and mutation for one target ref."""
    path = target_integration_lock_path(cwd, target_ref)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = open(path, 'a', encoding='utf-8')
    except OSError as error:
        raise BackendError(
            'could not open taskq integration lock {}: {}'.format(path, error)
        ) from error
    with lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def fast_forward_checked_out(cwd, expected_head, new_head):
    """Safely fast-forward a user checkout after a complete preflight.

    Ordinary dirty paths block landing. Ignored untracked paths are allowed
    unless an incoming tracked path would replace or structurally collide with
    them. Callers must hold ``target_integration_lock`` across this operation.
    """
    actual = resolve_commit(cwd)
    if actual != expected_head:
        raise LandingMoved(
            'destination moved from {} to {}'.format(
                str(expected_head)[:12], str(actual)[:12]))
    if not is_clean(cwd):
        raise LandingBlocked(
            'destination worktree is dirty; changes are waiting to land')
    collisions = path_collisions(
        ignored_untracked_paths(cwd),
        changed_paths_between(cwd, expected_head, new_head),
        ignore_case=core_ignorecase(cwd),
    )
    if collisions:
        raise LandingBlocked(
            'ignored local paths collide with incoming changes: {}'.format(
                ', '.join(collisions[:20])))
    # Repeat the cheap head check after scanning ignored files. A human Git
    # process is not covered by taskq's lock and may have advanced the branch.
    actual = resolve_commit(cwd)
    if actual != expected_head:
        raise LandingMoved(
            'destination moved from {} to {}'.format(
                str(expected_head)[:12], str(actual)[:12]))
    try:
        return merge_ff(cwd, new_head)
    except BackendError as error:
        raise LandingMoved(
            'destination could not be fast-forwarded: {}'.format(error)
        ) from error

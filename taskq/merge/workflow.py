"""Public registration hooks and sidecar projections for merge jobs."""

import json
import fcntl
import sqlite3
import sys
from pathlib import Path

from ..backends.base import BackendError
from ..git import (
    atomic_write, branch_worktrees, common_dir, delete_ref, is_ancestor,
    local_branch, repository_root, resolve_commit,
)
from .cleanup import cleanup_lane_if_idle, request_refs
from .config import build_merge_spec, lane_key, repository_key, state_root
from .state import MergeState, timestamp


def _plain(value):
    try:
        return json.loads(json.dumps(value))
    except (TypeError, ValueError) as error:
        raise BackendError('merge metadata must be JSON serializable') from error


def atomic_json(path, value):
    atomic_write(path, json.dumps(
        value, indent=2, sort_keys=True, separators=(',', ': '), default=str
    ) + '\n')


def read_json(path):
    if not path:
        return None
    try:
        with open(path, 'r', encoding='utf-8') as stream:
            return json.load(stream)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def request_job_ownership(request):
    """Return whether the current parent metadata owns *request*'s job path.

    Job IDs and their directories can be reused after a backend reset while
    terminal merge rows intentionally remain in the repository journal.  The
    per-submission UUID is therefore the authority for every sidecar write.
    ``None`` means the metadata is temporarily unreadable; callers must retry
    rather than projecting into an unverified directory.
    """
    spec = request.get('spec') or {}
    expected = spec.get('submission_id')
    parent = read_json(request.get('meta_file'))
    if parent is None:
        return None
    actual = parent.get('submission_id')
    return bool(expected) and actual == expected


def request_owns_job_dir(request):
    return request_job_ownership(request) is True


def sidecar_owns_request(request, payload):
    """Return whether a status/result payload belongs to *request*."""
    if not isinstance(payload, dict):
        return False
    projection = payload.get('merge')
    if not isinstance(projection, dict):
        projection = payload
    expected = (request.get('spec') or {}).get('submission_id')
    return bool(expected) and projection.get('submission_id') == expected


def command_result_owns_request(request, payload):
    """Return whether a command-result sidecar belongs to *request*."""
    expected = (request.get('spec') or {}).get('submission_id')
    return (
        bool(expected)
        and isinstance(payload, dict)
        and payload.get('submission_id') == expected
    )


def merge_data(meta):
    """Return a normalized merge mapping from a tmux job metadata object."""
    candidates = [
        meta.get('merge'),
        meta.get('merge_spec'),
        (meta.get('metadata') or {}).get('merge'),
        (meta.get('metadata') or {}).get('merge_spec'),
    ]
    raw = next((value for value in candidates if isinstance(value, dict)), None)
    if raw is None:
        raise BackendError('job metadata is missing its merge specification')
    result = dict(raw.get('spec') or {})
    result.update(raw)
    result.pop('spec', None)
    if not result.get('requested'):
        raise BackendError('job metadata does not request merge processing')
    return result


def _job_dir(meta, data):
    value = data.get('job_dir')
    if value:
        return Path(value).expanduser().resolve()
    wrapper = meta.get('wrapper')
    if wrapper:
        return Path(wrapper).expanduser().resolve().parent
    for key in ('command_result_file', 'merge_status_file', 'merge_result_file'):
        if meta.get(key):
            return Path(meta[key]).expanduser().resolve().parent
    raise BackendError('cannot determine merge job directory')


def _backend_spec(backend):
    return _plain({
        'name': getattr(backend, 'name', 'tmux'),
        'config': getattr(backend, 'config', {}),
        'state_dir': str(Path(backend.state_dir).resolve()),
        'queue': getattr(backend, 'config', {}).get('queue', 'default'),
        'controller_name': None,
    })


def controller_name(spec):
    return 'merge-{}'.format(spec['repository_key'])


def job_key_from_meta(meta, data=None, backend_state_dir=None):
    data = data or merge_data(meta)
    job_id = int(data.get('job_id', meta.get('id')))
    state_dir = backend_state_dir or data.get('backend_state_dir')
    if state_dir is None:
        job_dir = _job_dir(meta, data)
        # tmux state layout is <state>/jobs/<id>.
        state_dir = job_dir.parent.parent
    submission = (
        data.get('submission_id') or data.get('submission_token')
        or meta.get('submission_id') or meta.get('submission_token')
    )
    base = '{}:{}'.format(Path(state_dir).expanduser().resolve(), job_id)
    return '{}:{}'.format(base, submission) if submission else base


def projection(request, lane, target_head=None, **extra):
    stage = request['status']
    result = {
        'requested': True,
        'submission_id': (request.get('spec') or {}).get('submission_id'),
        'request_id': request.get('id'),
        'state': stage,
        'stage': stage,
        'lane': lane['id'],
        'sequence': request.get('sequence'),
        'destination': lane['target_branch'],
        'target_branch': lane['target_branch'],
        'target_ref': lane['target_ref'],
        'source_head': request['source_base'],
        'change_head': request.get('change_head'),
        'staged_head': request.get('staged_head'),
        'staging_head': lane.get('staging_head'),
        'target_head': target_head,
        'resolver_job_id': request.get('resolver_job_id'),
        'blocked_reason': lane.get('blocked_reason'),
        'error': request.get('error'),
    }
    result.update(extra)
    return {key: value for key, value in result.items() if value is not None}


def write_status(request, lane, target_head=None, **extra):
    if not request_owns_job_dir(request):
        return None
    value = projection(request, lane, target_head=target_head, **extra)
    atomic_json(request['status_file'], value)
    return value


def write_terminal(request, lane, success, target_head=None, error=None, **extra):
    if not request_owns_job_dir(request):
        return None
    value = projection(request, lane, target_head=target_head, **extra)
    value['state'] = value['stage'] = 'landed' if success else request['status']
    if error:
        value['error'] = error
    payload = {
        'status': 'success' if success else 'failed',
        'exitcode': 0 if success else None,
        'end_time': timestamp(),
        'merge': value,
    }
    if not success:
        payload['failure_phase'] = 'merge'
    if error:
        payload['error'] = error
    atomic_json(request['result_file'], payload)
    atomic_json(request['status_file'], value)
    return payload


def _delete_request_refs(request, repo_root):
    for ref in request_refs(request):
        try:
            delete_ref(repo_root, ref)
        except BackendError:
            pass


def register_merge_job(backend, meta):
    """Idempotently register an already-created tmux job for FIFO merging."""
    data = merge_data(meta)
    repo_root = repository_root(data.get('git_root') or meta.get('git_root'))
    git_common = common_dir(repo_root)
    target_branch, target_ref, target_head = local_branch(
        repo_root, data['target_branch'])
    checkouts = branch_worktrees(repo_root, target_ref)
    if checkouts:
        raise BackendError(
            'merge destination branch {!r} is checked out in {}'.format(
                target_branch,
                ', '.join(item['worktree'] for item in checkouts),
            ))
    expected_lane = lane_key(git_common, target_ref)
    if data.get('lane_id') not in {None, expected_lane}:
        raise BackendError('merge lane does not match repository destination')
    repo_state = state_root(git_common)
    state_path = repo_state / 'state.sqlite3'
    job_dir = _job_dir(meta, data)
    job_id = int(data.get('job_id', meta['id']))
    submission_id = data.get('submission_id') or meta.get('submission_id')
    if not isinstance(submission_id, str) or not submission_id:
        raise BackendError('merge job metadata is missing its submission ID')
    command_result = Path(
        data.get('command_result_file') or meta.get('command_result_file')
        or job_dir / 'command-result.json').resolve()
    status_file = Path(
        data.get('status_file') or meta.get('merge_status_file')
        or job_dir / 'merge-status.json').resolve()
    result_file = Path(
        data.get('result_file') or meta.get('merge_result_file')
        or job_dir / 'merge-result.json').resolve()
    source_worktree = Path(
        data.get('source_worktree') or meta.get('git_worktree') or repo_root
    ).expanduser().resolve()
    source_base = resolve_commit(
        repo_root,
        data.get('source_base') or meta.get('git_commit')
        or data.get('source_head') or 'HEAD',
    )
    spec = dict(data)
    spec.update({
        'version': int(data.get('version', 1)),
        'requested': True,
        'job_command': meta.get('command', ''),
        'repo_root': repo_root,
        'git_common_dir': git_common,
        'repository_key': repository_key(git_common),
        'lane_id': expected_lane,
        'target_branch': target_branch,
        'target_ref': target_ref,
        'target_head': data.get('target_head') or target_head,
        'state_path': str(state_path),
        'controller_lock': str(repo_state / 'controller.lock'),
        'controller_heartbeat': str(repo_state / 'controller.heartbeat'),
        'staging_ref': 'refs/taskq/merge/lanes/{}/staging'.format(expected_lane),
        'staging_worktree': str(repo_state / 'worktrees' / expected_lane),
        'job_id': job_id,
        'submission_id': submission_id,
        'job_dir': str(job_dir),
        'source_base': source_base,
        'source_worktree': str(source_worktree),
        'command_result_file': str(command_result),
        'status_file': str(status_file),
        'result_file': str(result_file),
    })
    spec = _plain(spec)
    backend_spec = _backend_spec(backend)
    backend_spec['controller_name'] = controller_name(spec)
    key = job_key_from_meta(
        meta, data=spec, backend_state_dir=backend_spec['state_dir'])
    lane = {
        'id': expected_lane,
        'repo_root': repo_root,
        'common_dir': git_common,
        'target_branch': target_branch,
        'target_ref': target_ref,
        'staging_ref': spec['staging_ref'],
        'staging_worktree': spec['staging_worktree'],
    }
    request_value = {
        'lane_id': expected_lane,
        'job_key': key,
        'parent_job_id': job_id,
        'job_dir': str(job_dir),
        'meta_file': str(job_dir / 'meta.json'),
        'command_result_file': str(command_result),
        'status_file': str(status_file),
        'result_file': str(result_file),
        'source_worktree': str(source_worktree),
        'source_base': source_base,
        'spec': spec,
        'backend': backend_spec,
    }
    if not hasattr(backend, 'register_controller'):
        raise BackendError('merge jobs require a backend controller')
    heartbeat = spec['controller_heartbeat']
    interval = max(0.1, float(backend.config.get('broker_interval', 1)))
    lock_path = repo_state / 'controller.lock'
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, 'a', encoding='utf-8') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with MergeState(state_path) as state:
            # Install the durable supervisor before publishing active DB work.
            # The schema already exists, and a controller started here cannot
            # observe false-idle shutdown because it must acquire this lock.
            backend.register_controller(
                backend_spec['controller_name'],
                [
                    sys.executable, '-m', 'taskq.merge.controller',
                    '--state', str(state_path), '--interval', str(interval),
                ],
                cwd=repo_root,
                heartbeat_file=heartbeat,
                timeout=max(30.0, interval * 5),
            )
            lane = state.ensure_lane(lane)
            request = state.add_request(request_value)
            write_status(request, lane, target_head=target_head)
        return request


def cancel_merge_job(meta, remove=False):
    """Best-effort cancellation hook used by kill/remove/reset paths."""
    try:
        data = merge_data(meta)
        git_common = common_dir(data.get('git_root') or meta.get('git_root'))
        path = Path(
            data.get('state_path') or state_root(git_common) / 'state.sqlite3'
        ).expanduser().resolve()
        if not path.exists():
            return None
        key = job_key_from_meta(meta, data=data)
        lock_path = path.parent / 'controller.lock'
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        # Serialize cancellation with the controller's final target-ref check
        # and landing.  Whichever wins determines the observable terminal
        # state; callers can therefore preserve success if landing won.
        with open(lock_path, 'a', encoding='utf-8') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with MergeState(path) as state:
                request = state.get_request_by_job_key(key)
                if request is None:
                    return None
                lane = state.get_lane(request['lane_id'])
                # Recover the target-update -> SQLite-finalization crash window
                # before honoring cancellation.  Once reachable, a staged
                # change has landed and kill must preserve success.
                if (
                    request['status'] == 'staged'
                    and request.get('staged_head')
                ):
                    try:
                        target_head = resolve_commit(
                            lane['repo_root'], lane['target_ref'])
                    except BackendError:
                        target_head = None
                    if target_head and is_ancestor(
                        lane['repo_root'], request['staged_head'], target_head
                    ):
                        request = state.finish_request(
                            request['id'], 'landed', result={
                                'head': target_head,
                                'manual_or_recovered': True,
                            })
                        if not remove and Path(request['result_file']).parent.exists():
                            write_terminal(
                                request, lane, True, target_head=target_head,
                                recovered=True, command_exitcode=0,
                            )
                        if remove:
                            _delete_request_refs(request, lane['repo_root'])
                        cleanup_lane_if_idle(state, lane)
                        return request
                previous_status = request['status']
                request = state.cancel_job(key)
                if Path(request['status_file']).parent.exists():
                    try:
                        write_status(request, lane, cancelled=True)
                    except OSError:
                        pass
                if remove:
                    _delete_request_refs(request, lane['repo_root'])
                if previous_status != 'resolving':
                    cleanup_lane_if_idle(state, lane)
                return request
    except (BackendError, OSError, ValueError, sqlite3.Error):
        return None


def cleanup_merge_job(meta):
    """Synchronously remove idle staging after resolver sessions are stopped."""
    data = merge_data(meta)
    git_common = common_dir(data.get('git_root') or meta.get('git_root'))
    path = Path(
        data.get('state_path') or state_root(git_common) / 'state.sqlite3'
    ).expanduser().resolve()
    if not path.exists():
        return False
    key = job_key_from_meta(meta, data=data)
    lock_path = path.parent / 'controller.lock'
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, 'a', encoding='utf-8') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with MergeState(path) as state:
            request = state.get_request_by_job_key(key)
            if request is None:
                return False
            lane = state.get_lane(request['lane_id'])
            return cleanup_lane_if_idle(state, lane)

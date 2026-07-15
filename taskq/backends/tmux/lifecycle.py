"""Pure metadata transitions for tmux command and merge sidecars."""

import json
import os
import uuid
from pathlib import Path


TERMINAL_STATUSES = {'success', 'failed', 'killed', 'interrupted'}


def atomic_json(path, value):
    """Atomically write JSON using a same-directory, unique temporary file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name('.{}.{}.{}.tmp'.format(
        path.name, os.getpid(), uuid.uuid4().hex))
    try:
        with open(temporary, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write('\n')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_sidecar(path):
    if not path:
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            value = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def command_result(meta):
    result = read_sidecar(meta.get('command_result_file'))
    if not result:
        return None
    submission_id = meta.get('submission_id')
    if submission_id and result.get('submission_id') != submission_id:
        return None
    try:
        result = dict(result)
        result['exitcode'] = int(result['exitcode'])
    except (KeyError, TypeError, ValueError):
        return None
    return result


def command_exitcode(meta):
    result = command_result(meta)
    return result['exitcode'] if result else None


def finish_command(meta, exitcode, timestamp):
    """Apply a completed command to metadata.

    A successful merge-enabled command remains nonterminal.  Everything else
    follows the ordinary tmux lifecycle.
    """
    exitcode = int(exitcode)
    merge = meta.get('merge')
    if isinstance(merge, dict):
        meta['command_exitcode'] = exitcode
        merge['command_exitcode'] = exitcode
    if isinstance(merge, dict) and exitcode == 0:
        meta.update({
            'status': 'merging',
            'exitcode': None,
            'pid': None,
        })
        initial = {None, 'waiting', 'awaiting_command'}
        if merge.get('state') in initial:
            merge['state'] = 'queued'
        if merge.get('stage') in initial:
            merge['stage'] = 'queued'
        merge.setdefault('command_finished_at', timestamp)
        return
    meta.update({
        'status': 'success' if exitcode == 0 else 'failed',
        'exitcode': exitcode,
        'end_time': meta.get('end_time') or timestamp,
    })
    if isinstance(merge, dict) and exitcode != 0:
        meta['failure_phase'] = 'command'
        merge.update({'state': 'skipped', 'stage': 'skipped'})


def _merge_payload(payload):
    nested = payload.get('merge') if payload else None
    if isinstance(nested, dict):
        return nested
    return payload if isinstance(payload, dict) else None


def _owned_merge_payload(meta, payload):
    """Return a sidecar projection only when its submission UUID matches."""
    projection = _merge_payload(payload)
    merge = meta.get('merge')
    expected = meta.get('submission_id')
    if expected is None and isinstance(merge, dict):
        expected = merge.get('submission_id')
    if (
        not expected
        or not isinstance(projection, dict)
        or projection.get('submission_id') != expected
    ):
        return None
    return projection


def _update_merge(meta, payload):
    projection = _merge_payload(payload)
    if not projection:
        return False
    merge = meta.setdefault('merge', {})
    before = dict(merge)
    merge.update(projection)
    return merge != before


def refresh_merge(meta, timestamp):
    """Consume controller projections and a terminal merge result."""
    merge = meta.get('merge')
    if not isinstance(merge, dict):
        return False
    status = read_sidecar(
        meta.get('merge_status_file') or merge.get('status_file'))
    status_projection = _owned_merge_payload(meta, status)
    changed = _update_merge(meta, status_projection)
    result = read_sidecar(
        meta.get('merge_result_file') or merge.get('result_file'))
    result_projection = _owned_merge_payload(meta, result)
    if result_projection is None:
        result = None
    if not result:
        if (
            meta.get('status') == 'merging'
            and isinstance(status_projection, dict)
            and status_projection.get('cancelled') is True
            and status_projection.get('stage') == 'cancelled'
        ):
            # Cancellation is durable in the merge DB before ``tq kill`` can
            # publish the parent transition.  Recover that crash window (and
            # stale broker writes) without reclassifying ordinary command
            # failures, which are already terminal before they reach here.
            meta.update({
                'status': 'killed',
                'exitcode': -1,
                'end_time': meta.get('end_time') or timestamp,
                'pid': None,
            })
            return True
        return changed
    changed = _update_merge(meta, result_projection) or changed
    terminal = result.get('status')
    if terminal not in TERMINAL_STATUSES:
        return changed
    old = (
        meta.get('status'), meta.get('exitcode'), meta.get('end_time'),
        meta.get('failure_phase'),
    )
    if terminal == 'success':
        exitcode = result.get('exitcode', 0)
    elif terminal == 'killed':
        exitcode = result.get('exitcode', -1)
    else:
        exitcode = result.get('exitcode')
    meta.update({
        'status': terminal,
        'exitcode': exitcode,
        'end_time': result.get('end_time') or meta.get('end_time') or timestamp,
        'pid': None,
    })
    if terminal == 'failed':
        meta['failure_phase'] = result.get('failure_phase') or 'merge'
    error = result.get('error')
    if error:
        meta['merge']['error'] = error
    new = (
        meta.get('status'), meta.get('exitcode'), meta.get('end_time'),
        meta.get('failure_phase'),
    )
    return changed or new != old


def sidecar_paths(job_dir):
    job_dir = Path(job_dir)
    return {
        'command_result_file': str(job_dir / 'command-result.json'),
        'merge_status_file': str(job_dir / 'merge-status.json'),
        'merge_result_file': str(job_dir / 'merge-result.json'),
    }

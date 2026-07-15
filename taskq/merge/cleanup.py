"""Shared cleanup for taskq-owned standalone merge artifacts."""

from pathlib import Path

from ..git import delete_ref, remove_worktree


def request_refs(request):
    """Return every deterministic ref owned by a merge request."""
    values = [
        request.get('change_ref'),
        'refs/taskq/merge/requests/{}/resolved'.format(request['id']),
    ]
    result = request.get('result') or {}
    if isinstance(result, dict):
        values.append(result.get('resolved_ref'))
    return list(dict.fromkeys(value for value in values if value))


def delete_request_refs(repo_root, request):
    for ref in request_refs(request):
        delete_ref(repo_root, ref)


def cleanup_lane_if_idle(state, lane):
    """Remove a lane's owned worktree/refs when no request can still land."""
    if state.list_requests(lane['id'], include_terminal=False):
        return False
    path = Path(lane['staging_worktree'])
    if path.exists():
        remove_worktree(lane['repo_root'], path, force=True)
    delete_ref(lane['repo_root'], lane['staging_ref'])
    for request in state.list_requests(lane['id']):
        # Failed candidates remain inspectable until their parent is removed.
        if request['status'] != 'failed':
            delete_request_refs(lane['repo_root'], request)
    state.update_lane(
        lane['id'], staging_head=None, needs_rebuild=True,
        blocked_reason=None)
    return True

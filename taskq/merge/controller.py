"""Crash-safe controller for repository-scoped standalone merge FIFOs."""

import argparse
import fcntl
import json
import shlex
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from string import Template

from ..backends.base import BackendError
from ..backends.tmux.backend import TmuxBackend
from ..git import (
    abort_cherry_pick,
    add_detached_worktree,
    branch_worktrees,
    cherry_pick,
    cherry_pick_in_progress,
    git,
    is_ancestor,
    is_clean,
    local_branch,
    local_branch_ref,
    reset_hard,
    resolve_commit,
    synthetic_change_commit,
    update_ref,
)
from ..integration import (
    target_integration_lock,
)
from .state import ACTIVE_REQUEST_STATES, MergeState, TERMINAL_REQUEST_STATES
from .cleanup import cleanup_lane_if_idle
from .workflow import (
    command_result_owns_request,
    read_json,
    request_job_ownership,
    request_owns_job_dir,
    sidecar_owns_request,
    write_status,
    write_terminal,
)


TERMINAL_JOB_STATES = frozenset({'success', 'failed', 'killed', 'interrupted'})


def _plain(value):
    return json.loads(json.dumps(value))


class MergeController:
    """One idempotent reconciliation pass over every branch lane in a DB."""

    def __init__(self, state, backend_factory=None):
        self.state = state
        self.backend_factory = backend_factory or self._default_backend_factory
        self._backends = {}

    @staticmethod
    def _default_backend_factory(spec):
        if spec.get('name') != 'tmux':
            raise BackendError(
                'unsupported merge backend: {}'.format(spec.get('name')))
        return TmuxBackend('tmux', _plain(spec['config']))

    def _backend(self, request):
        spec = request['backend']
        key = spec.get('state_dir') or json.dumps(spec, sort_keys=True)
        if key not in self._backends:
            self._backends[key] = self.backend_factory(spec)
        return self._backends[key]

    @property
    def heartbeat_path(self):
        return self.state.path.parent / 'controller.heartbeat'

    @property
    def lock_path(self):
        return self.state.path.parent / 'controller.lock'

    def reconcile(self):
        self.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.lock_path, 'a', encoding='utf-8') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            # Only the process that owns the repository reconciliation lock is
            # healthy enough to heartbeat.  A polling nonleader must not hide a
            # wedged leader from the tmux watchdog.
            self.heartbeat_path.touch()
            self._reconcile_locked()
            return True

    def _reconcile_locked(self):
        self._collect_command_results()
        self._cancel_orphaned_resolvers()
        self._reconcile_resolvers()
        for lane in self.state.list_lanes(active_only=True):
            try:
                self._advance_lane(lane)
            except BackendError as error:
                self._block_lane(lane, str(error))
        self._project_active()
        self._recover_terminal_sidecars()
        self._cleanup_idle_lanes()

    def _collect_command_results(self):
        ready = []
        for request in self.state.list_requests(statuses={'waiting'}):
            result = read_json(request['command_result_file'])
            if not command_result_owns_request(request, result):
                result = None
            if not isinstance(result, dict) or 'exitcode' not in result:
                parent = read_json(request.get('meta_file'))
                if parent is None:
                    meta_path = request.get('meta_file')
                    if not meta_path or not Path(meta_path).exists():
                        self.state.finish_request(
                            request['id'], 'cancelled', result={
                                'merge_skipped': True,
                                'reason': 'parent job metadata was removed',
                            })
                    continue
                parent_status = parent.get('status')
                if parent_status in {
                    'success', 'failed', 'killed', 'interrupted',
                }:
                    self.state.finish_request(
                        request['id'], 'cancelled', result={
                            'command_exitcode': parent.get('exitcode'),
                            'merge_skipped': True,
                            'parent_status': parent_status,
                            'reason': 'atomic command-result sidecar is missing',
                        })
                    continue
                if parent_status == 'merging':
                    self._fail_request(
                        request,
                        'atomic command-result sidecar is missing after command '
                        'completion',
                        merge_failure=True,
                    )
                    continue
                if parent_status != 'running':
                    continue
                # A running command can print arbitrary text that resembles
                # taskq's legacy completion marker. Only the wrapper's atomic
                # handoff is authoritative for a merge request.
                continue
            if not isinstance(result, dict) or 'exitcode' not in result:
                continue
            try:
                exitcode = int(result['exitcode'])
            except (TypeError, ValueError):
                self._fail_request(
                    request, 'invalid command-result sidecar', merge_failure=True)
                continue
            if exitcode:
                # The ordinary command lifecycle owns this failure.  Retire the
                # merge request without replacing the command's real exit code.
                self.state.finish_request(
                    request['id'], 'cancelled',
                    result={'command_exitcode': exitcode, 'merge_skipped': True},
                )
                lane = self.state.get_lane(request['lane_id'])
                self._cleanup_lane_if_idle(lane)
                continue
            try:
                mtime = Path(request['command_result_file']).stat().st_mtime_ns
            except OSError:
                mtime = 0
            ready.append((mtime, request['id'], request))
        for _, _, request in sorted(ready):
            # Another controller can have won between the scan and this pass.
            request = self.state.get_request(request['id'])
            if request is None or request['status'] != 'waiting':
                continue
            self._prepare_change(request)

    def _prepare_change(self, request):
        lane = self.state.get_lane(request['lane_id'])
        spec = request['spec']
        ref = 'refs/taskq/merge/requests/{}/change'.format(request['id'])
        message = 'taskq merge job {}\n\nTaskq-Merge-Request: {}'.format(
            request['parent_job_id'], request['id'])
        try:
            change, tree = synthetic_change_commit(
                request['source_worktree'], request['source_base'], message)
            if change:
                update_ref(lane['repo_root'], ref, change)
            request = self.state.mark_ready(
                request['id'], ref if change else None, change, tree)
        except (BackendError, OSError) as error:
            self._fail_request(
                request, 'could not snapshot job changes: {}'.format(error),
                merge_failure=True,
            )
            return
        if request['status'] == 'landed':
            target = self._target_head(lane, block=False)
            self._parent_log(request, 'no changes to merge; completed')
            write_terminal(
                request, lane, True, target_head=target,
                command_exitcode=0, noop=True,
            )
            self._cleanup_lane_if_idle(lane)
        else:
            self._parent_log(
                request, 'queued as FIFO sequence {}'.format(request['sequence']))
            write_status(request, lane, target_head=self._target_head(lane, block=False))

    def _target_head(self, lane, block=True):
        result = git(
            lane['repo_root'], 'show-ref', '--verify', '--hash',
            lane['target_ref'], check=False)
        if result.returncode:
            if block:
                self._block_lane(lane, 'destination branch was deleted')
            return None
        return result.stdout.strip()

    def _advance_lane(self, lane):
        lane = self.state.get_lane(lane['id'])
        target_head = self._target_head(lane)
        if not target_head:
            return
        if self._landed_history_was_removed(lane, target_head):
            self._block_lane(
                lane, 'destination branch rewrite removed already-landed taskq changes')
            return
        lane = self._recognize_manual_landing(lane, target_head)
        active = self.state.list_requests(
            lane['id'], statuses=ACTIVE_REQUEST_STATES - {'waiting'})
        if not active:
            self.state.update_lane(
                lane['id'], base_head=target_head, blocked_reason=None)
            self._cleanup_lane_if_idle(lane)
            return
        if any(request['status'] == 'resolving' for request in active):
            # Preserve the resolver's in-progress index even if the target moved;
            # its result will be replayed (or rejected) on the next pass.
            return
        if any(request['status'] == 'applying' for request in active):
            lane = self.state.update_lane(lane['id'], needs_rebuild=True)
        if lane.get('base_head') and lane['base_head'] != target_head:
            lane = self.state.update_lane(lane['id'], needs_rebuild=True)
        if not self._staging_consistent(lane):
            lane = self.state.update_lane(lane['id'], needs_rebuild=True)
        if lane['needs_rebuild']:
            self._rebuild_lane(lane, target_head)
            lane = self.state.get_lane(lane['id'])
            if self.state.list_requests(lane['id'], statuses={'resolving'}):
                return
        else:
            self._ensure_staging(lane, target_head)
        self._apply_queued(lane)
        if self.state.list_requests(
            lane['id'], statuses={'queued', 'applying', 'resolving'}):
            return
        self._land_staged(self.state.get_lane(lane['id']))

    def _landed_history_was_removed(self, lane, target_head):
        landed = self.state.list_requests(lane['id'], statuses={'landed'})
        commits = [request['staged_head'] for request in landed
                   if request.get('staged_head')]
        return bool(commits and not is_ancestor(
            lane['repo_root'], commits[-1], target_head))

    def _recognize_manual_landing(self, lane, target_head):
        recognized = False
        requests = self.state.list_requests(
            lane['id'], statuses={'queued', 'applying', 'resolving', 'staged'})
        for request in requests:
            if request['status'] != 'staged' or not request.get('staged_head'):
                break
            if not is_ancestor(lane['repo_root'], request['staged_head'], target_head):
                break
            request = self.state.finish_request(
                request['id'], 'landed', result={
                    'head': target_head, 'manual_or_recovered': True})
            write_terminal(
                request, lane, True, target_head=target_head,
                command_exitcode=0, recovered=True,
            )
            self._parent_log(
                request, 'destination already contains the staged change; completed')
            recognized = True
        if recognized:
            lane = self.state.update_lane(lane['id'], needs_rebuild=True)
        return lane

    def _staging_consistent(self, lane):
        path = Path(lane['staging_worktree'])
        if not path.exists():
            return not lane.get('staging_head')
        if not (path / '.git').exists():
            return False
        try:
            worktree_head = resolve_commit(path)
            ref_head = resolve_commit(lane['repo_root'], lane['staging_ref'])
        except BackendError:
            return False
        expected = lane.get('staging_head')
        return worktree_head == ref_head and (expected is None or expected == ref_head)

    def _ensure_staging(self, lane, target_head):
        path = Path(lane['staging_worktree'])
        if path.exists() and not (path / '.git').exists():
            shutil.rmtree(path, ignore_errors=True)
        if not path.exists():
            update_ref(lane['repo_root'], lane['staging_ref'], target_head)
            try:
                add_detached_worktree(
                    lane['repo_root'], path, lane['staging_ref'])
            except BackendError:
                git(lane['repo_root'], 'worktree', 'prune', check=False)
                if path.exists():
                    shutil.rmtree(path, ignore_errors=True)
                add_detached_worktree(
                    lane['repo_root'], path, lane['staging_ref'])
        if lane.get('staging_head') is None:
            reset_hard(path, target_head)
            update_ref(lane['repo_root'], lane['staging_ref'], target_head)
            self.state.update_lane(
                lane['id'], base_head=target_head, staging_head=target_head,
                needs_rebuild=False, blocked_reason=None,
            )
        return str(path)

    def _rebuild_lane(self, lane, target_head):
        path = Path(lane['staging_worktree'])
        if path.exists() and (path / '.git').exists():
            abort_cherry_pick(path)
            reset_hard(path, target_head)
        else:
            if path.exists():
                shutil.rmtree(path, ignore_errors=True)
            update_ref(lane['repo_root'], lane['staging_ref'], target_head)
            add_detached_worktree(
                lane['repo_root'], path, lane['staging_ref'])
        update_ref(lane['repo_root'], lane['staging_ref'], target_head)
        self.state.update_lane(
            lane['id'], base_head=target_head, staging_head=target_head,
            needs_rebuild=False, blocked_reason=None,
        )
        for request in self.state.list_requests(
            lane['id'], statuses={'queued', 'applying', 'staged'}):
            self.state.update_request(
                request['id'], status='queued', staged_head=None,
                resolver_job_id=None,
            )
        self._apply_queued(self.state.get_lane(lane['id']))

    def _apply_queued(self, lane):
        for request in self.state.list_requests(lane['id'], statuses={'queued'}):
            if not self._apply_request(self.state.get_lane(lane['id']), request):
                return

    def _apply_request(self, lane, request):
        path = lane['staging_worktree']
        before = resolve_commit(path)
        request = self.state.update_request(
            request['id'], status='applying', staged_head=None)
        replay_head = (
            (request.get('result') or {}).get('resolved_head')
            or request['change_head']
        )
        clean, detail = cherry_pick(path, replay_head)
        if not clean:
            # Git reports an already-covered/empty cherry-pick as nonzero while
            # leaving CHERRY_PICK_HEAD with a clean index.  It is a valid staged
            # no-op, not a semantic conflict requiring an agent.
            if cherry_pick_in_progress(path) and is_clean(path):
                git(path, 'cherry-pick', '--skip')
                clean = True
            else:
                clean = False
        if not clean:
            if request['resolver_attempts'] >= 1:
                abort_cherry_pick(path)
                reset_hard(path, before)
                self._fail_request(
                    request,
                    'change conflicted again after its one resolver attempt',
                    merge_failure=True,
                )
                self.state.update_lane(
                    lane['id'], staging_head=before, needs_rebuild=False)
                return True
            # A live resolver owns the staging sequencer, so the lane must
            # pause.  If the resolver could not be queued, only this request
            # is failed and the remaining FIFO can keep advancing.
            return not self._start_resolver(lane, request, before, detail)
        head = resolve_commit(path)
        update_ref(lane['repo_root'], lane['staging_ref'], head)
        request = self.state.update_request(
            request['id'], status='staged', staged_head=head)
        self.state.update_lane(
            lane['id'], staging_head=head, needs_rebuild=False,
            blocked_reason=None)
        write_status(
            request, self.state.get_lane(lane['id']),
            target_head=self._target_head(lane, block=False))
        self._parent_log(request, 'change staged; waiting to land')
        return True

    def _start_resolver(self, lane, request, before, detail):
        token = request.get('resolver_token') or uuid.uuid4().hex
        attempts = int(request['resolver_attempts']) + 1
        request = self.state.update_request(
            request['id'], status='resolving', resolver_token=token,
            resolver_attempts=attempts, resolver_job_id=None,
            result={'pre_resolve_head': before, 'conflict': detail[-4000:]},
        )
        prompt = self._resolver_prompt(lane, request)
        command = [
            prompt if arg == '{}' else arg
            for arg in request['spec']['resolver']['command']
        ]
        job_id = self._queue_resolver(lane, request, command)
        if job_id is None:
            return False
        request = self.state.update_request(
            request['id'], resolver_job_id=int(job_id))
        self._parent_log(
            request, 'conflict detected; resolver job {} started'.format(job_id))
        write_status(
            request, lane, target_head=self._target_head(lane, block=False),
            resolver_token=token,
        )
        return True

    def _resolver_prompt(self, lane, request):
        parent = read_json(request.get('meta_file')) or {}
        values = {
            'repo_root': lane['repo_root'],
            'target_branch': lane['target_branch'],
            'target_head': lane.get('base_head') or '',
            'job_id': request['parent_job_id'],
            'change_head': request.get('change_head') or '',
            'command': parent.get('command', ''),
            'destination': json.dumps({
                'branch': lane['target_branch'],
                'head': lane.get('base_head'),
                'repository': lane['repo_root'],
            }, sort_keys=True),
            'change': json.dumps({
                'commit': request.get('change_head'),
                'parent_job_id': request['parent_job_id'],
                'source_base': request['source_base'],
            }, sort_keys=True),
            'artifacts': json.dumps(request.get('result') or {}, sort_keys=True),
        }
        return Template(
            request['spec']['resolver']['conflict_prompt']
        ).safe_substitute(values).strip()

    def _reconcile_resolvers(self):
        for request in self.state.list_requests(statuses={'resolving'}):
            lane = self.state.get_lane(request['lane_id'])
            backend = self._backend(request)
            job_id = request.get('resolver_job_id')
            info = None
            if job_id is not None:
                rows = backend.full_info([int(job_id)])
                info = rows[0] if rows else None
            if info is None:
                info = self._adopt_resolver(backend, request)
                if info is not None:
                    request = self.state.update_request(
                        request['id'], resolver_job_id=int(info['id']))
            if info is None and request.get('resolver_job_id') is None:
                # Crash recovery for the state-update -> backend.add window.
                self._launch_existing_resolver(lane, request)
                continue
            if info is None:
                self._finish_resolver(
                    lane, request,
                    {'id': request['resolver_job_id'], 'status': 'interrupted'},
                )
                continue
            if info.get('status') not in TERMINAL_JOB_STATES:
                if info is not None and self._resolver_timed_out(request, info):
                    backend.kill({'id': int(info['id'])})
                    self._finish_resolver(
                        lane, request, info, timed_out=True)
                continue
            self._finish_resolver(lane, request, info)

    def _launch_existing_resolver(self, lane, request):
        prompt = self._resolver_prompt(lane, request)
        command = [
            prompt if arg == '{}' else arg
            for arg in request['spec']['resolver']['command']
        ]
        job_id = self._queue_resolver(lane, request, command)
        if job_id is not None:
            self.state.update_request(request['id'], resolver_job_id=int(job_id))

    def _queue_resolver(self, lane, request, command):
        backend = self._backend(request)
        metadata = {
            'role': 'merge-resolver',
            'merge_resolver': {
                'token': request['resolver_token'],
                'request_id': request['id'],
                'state_path': str(self.state.path),
            },
        }
        try:
            return backend.add(
                shlex.join(command), gpus=0, slots=1,
                env=self._resolver_environment(request),
                cwd=str(lane['staging_worktree']),
                workspace_owner='merge', metadata=metadata,
            )
        except (BackendError, OSError, TypeError, ValueError) as error:
            # backend.add can fail after publishing the resolver job.  Adopt
            # that token before declaring failure, preserving the existing
            # state-update crash-recovery invariant.
            try:
                adopted = self._adopt_resolver(backend, request)
            except (BackendError, OSError, TypeError, ValueError):
                adopted = None
            if adopted is not None:
                return int(adopted['id'])
            self._fail_resolver_launch(lane, request, error)
            return None

    def _fail_resolver_launch(self, lane, request, error):
        before = (request.get('result') or {}).get('pre_resolve_head')
        cleanup_error = None
        try:
            abort_cherry_pick(lane['staging_worktree'])
            if before:
                reset_hard(lane['staging_worktree'], before)
                update_ref(lane['repo_root'], lane['staging_ref'], before)
        except (BackendError, OSError) as cleanup:
            cleanup_error = cleanup
        self.state.update_lane(
            lane['id'], staging_head=before, needs_rebuild=True)
        reason = 'could not launch merge conflict resolver: {}'.format(error)
        if cleanup_error is not None:
            reason += '; staging cleanup will be retried: {}'.format(cleanup_error)
        self._fail_request(request, reason, merge_failure=True)

    @staticmethod
    def _resolver_environment(request):
        parent = read_json(request.get('meta_file')) or {}
        value = read_json(parent.get('env_file'))
        if not isinstance(value, dict):
            value = {}
        else:
            value = dict(value)
        # The parent command's ordinary environment is inherited, but Git
        # routing variables must never redirect a conflict agent away from the
        # taskq-owned staging worktree/index/object database.
        for key in (
            'GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR',
            'GIT_OBJECT_DIRECTORY', 'GIT_ALTERNATE_OBJECT_DIRECTORIES',
            'GIT_NAMESPACE', 'GIT_PREFIX', 'GIT_CEILING_DIRECTORIES',
            'GIT_QUARANTINE_PATH', 'GIT_CONFIG', 'GIT_CONFIG_SYSTEM',
            'GIT_CONFIG_GLOBAL', 'GIT_CONFIG_NOSYSTEM',
            'GIT_CONFIG_PARAMETERS',
        ):
            # None is encoded by the tmux backend as an explicit shell unset;
            # merely omitting the key would leak the tmux server's environment.
            value[key] = None
        try:
            count = int(value.get('GIT_CONFIG_COUNT', 0))
        except (TypeError, ValueError):
            count = 0
        overrides = (
            ('user.name', 'taskq'),
            ('user.email', 'taskq@localhost'),
            ('commit.gpgSign', 'false'),
            ('core.hooksPath', '/dev/null'),
        )
        for offset, (key, setting) in enumerate(overrides):
            value['GIT_CONFIG_KEY_{}'.format(count + offset)] = key
            value['GIT_CONFIG_VALUE_{}'.format(count + offset)] = setting
        value['GIT_CONFIG_COUNT'] = str(count + len(overrides))
        return value

    def _cancel_orphaned_resolvers(self):
        for request in self.state.list_requests(statuses={'cancelled', 'failed'}):
            job_id = request.get('resolver_job_id')
            if job_id is None:
                continue
            try:
                backend = self._backend(request)
                rows = backend.full_info([int(job_id)])
                if rows and rows[0].get('status') not in TERMINAL_JOB_STATES:
                    backend.kill({'id': int(job_id)})
            except (BackendError, OSError):
                continue

    def _adopt_resolver(self, backend, request):
        token = request.get('resolver_token')
        if not token:
            return None
        for info in backend.full_info(None):
            metadata = info.get('metadata') or {}
            resolver = metadata.get('merge_resolver') or {}
            if resolver.get('token') == token:
                return info
        return None

    @staticmethod
    def _resolver_timed_out(request, info):
        timeout = float(request['spec']['resolver'].get('timeout', 0))
        if timeout <= 0 or not info.get('start_time'):
            return False
        start = info['start_time']
        if isinstance(start, str):
            start = datetime.fromisoformat(start.replace('Z', '+00:00'))
        if start.tzinfo is None:
            # tmux metadata historically stores naive local wall-clock time.
            # Attach the host's local zone before comparing with UTC.
            start = start.astimezone()
        return (datetime.now(timezone.utc) - start).total_seconds() >= timeout

    def _finish_resolver(self, lane, request, info, timed_out=False):
        path = lane['staging_worktree']
        output = ''
        try:
            output = self._backend(request).output({'id': int(info['id'])}, 0)
        except (BackendError, OSError):
            pass
        before = (request.get('result') or {}).get('pre_resolve_head')
        valid = (
            not timed_out
            and info.get('status') == 'success'
            and not cherry_pick_in_progress(path)
            and is_clean(path)
        )
        if valid:
            head = resolve_commit(path)
            valid = not before or self._valid_resolved_change(
                path, before, head, request)
        if not valid:
            abort_cherry_pick(path)
            if before:
                reset_hard(path, before)
                update_ref(lane['repo_root'], lane['staging_ref'], before)
                self.state.update_lane(
                    lane['id'], staging_head=before, needs_rebuild=True)
            reason = (
                'merge conflict resolver timed out'
                if timed_out else
                'merge conflict resolver did not produce a completed clean cherry-pick'
            )
            if output.strip():
                reason += ': ' + output.strip()[-4000:]
            self._fail_request(request, reason, merge_failure=True)
            return
        resolved_ref = 'refs/taskq/merge/requests/{}/resolved'.format(
            request['id'])
        update_ref(lane['repo_root'], resolved_ref, head)
        update_ref(lane['repo_root'], lane['staging_ref'], head)
        result = dict(request.get('result') or {})
        result.update({
            'resolved': True,
            'resolved_head': head,
            'resolved_ref': resolved_ref,
        })
        request = self.state.update_request(
            request['id'], status='staged', staged_head=head,
            resolver_job_id=int(info['id']), result=result)
        self.state.update_lane(
            lane['id'], staging_head=head, needs_rebuild=False,
            blocked_reason=None)
        write_status(
            request, self.state.get_lane(lane['id']),
            target_head=self._target_head(lane, block=False))
        self._parent_log(request, 'conflict resolved; change staged')

    @staticmethod
    def _valid_resolved_change(path, before, head, request):
        if head == before or not is_ancestor(path, before, head):
            return False
        try:
            count = int(git(path, 'rev-list', '--count', '{}..{}'.format(
                before, head)))
            before_tree = git(path, 'rev-parse', '{}^{{tree}}'.format(before))
            head_tree = git(path, 'rev-parse', '{}^{{tree}}'.format(head))
            message = git(path, 'show', '-s', '--format=%B', head)
        except (BackendError, ValueError):
            return False
        trailer = 'Taskq-Merge-Request: {}'.format(request['id'])
        return count == 1 and before_tree != head_tree and trailer in message

    def _land_staged(self, lane):
        # Explore campaigns and standalone merge queues have different FIFO
        # state machines, but both use this repository/ref landing lock so
        # their target-head preflight and ref mutation cannot race each other.
        with target_integration_lock(lane['repo_root'], lane['target_ref']):
            return self._land_staged_locked(lane)

    @staticmethod
    def _landing_ref_error(lane):
        try:
            _, validated_ref = local_branch_ref(
                lane['repo_root'], lane['target_branch'])
            if validated_ref != lane['target_ref']:
                raise BackendError(
                    'destination ref changed from {} to {}'.format(
                        lane['target_ref'], validated_ref))
        except BackendError as error:
            return str(error)
        return None

    def _fail_unsafe_staged(self, lane, staged, error):
        reason = (
            'destination branch {!r} became unsafe after submission: {}; '
            'refusing automatic landing'.format(lane['target_branch'], error))
        for request in staged:
            self._fail_request(request, reason, merge_failure=True)

    def _land_staged_locked(self, lane):
        staged = self.state.list_requests(lane['id'], statuses={'staged'})
        if not staged:
            return
        error = self._landing_ref_error(lane)
        if error:
            self._fail_unsafe_staged(lane, staged, error)
            return
        target_head = self._target_head(lane)
        if not target_head:
            return
        if target_head != lane.get('base_head'):
            self.state.update_lane(lane['id'], needs_rebuild=True)
            return
        staging_head = resolve_commit(lane['repo_root'], lane['staging_ref'])
        if staging_head != lane.get('staging_head'):
            self.state.update_lane(lane['id'], needs_rebuild=True)
            return
        checkouts = branch_worktrees(lane['repo_root'], lane['target_ref'])
        if checkouts:
            paths = ', '.join(item['worktree'] for item in checkouts)
            reason = (
                'destination branch {!r} became checked out in {} after '
                'submission; refusing automatic landing'.format(
                    lane['target_branch'], paths))
            # This cannot be a transient wait: automatically landing after the
            # user later switches away would make that unrelated checkout
            # action trigger a surprising branch advance. A fresh `tq add`
            # submission is the explicit retry.
            for request in staged:
                self._fail_request(request, reason, merge_failure=True)
            return
        # Revalidate immediately before the compare-and-swap. In particular,
        # do not trust a case-canonical/direct-ref check made before the
        # staging and worktree preflights above.
        error = self._landing_ref_error(lane)
        if error:
            self._fail_unsafe_staged(lane, staged, error)
            return
        if staging_head == target_head:
            landed_head = target_head
        else:
            try:
                landed_head = update_ref(
                    lane['repo_root'], lane['target_ref'], staging_head,
                    old=target_head, message='taskq merge FIFO landing',
                    no_deref=True,
                )
            except BackendError:
                self.state.update_lane(lane['id'], needs_rebuild=True)
                return
        lane = self.state.update_lane(
            lane['id'], base_head=landed_head, staging_head=landed_head,
            needs_rebuild=False, blocked_reason=None)
        for request in staged:
            if not request.get('staged_head') or not is_ancestor(
                lane['repo_root'], request['staged_head'], landed_head):
                break
            request = self.state.finish_request(
                request['id'], 'landed', result={'head': landed_head})
            write_terminal(
                request, lane, True, target_head=landed_head,
                command_exitcode=0,
            )
            self._parent_log(
                request, 'landed on {} at {}'.format(
                    lane['target_branch'], landed_head[:12]))
        self._cleanup_lane_if_idle(lane)

    def _fail_request(self, request, reason, merge_failure):
        request = self.state.get_request(request['id']) or request
        if request['status'] in TERMINAL_REQUEST_STATES:
            return request
        request = self.state.finish_request(
            request['id'], 'failed', result={'failure_phase': 'merge'}, error=reason)
        lane = self.state.get_lane(request['lane_id'])
        self._parent_log(request, 'failed: {}'.format(reason))
        if merge_failure:
            try:
                write_terminal(
                    request, lane, False,
                    target_head=self._target_head(lane, block=False),
                    error=reason, command_exitcode=0,
                )
            except OSError:
                pass
        self._cleanup_lane_if_idle(lane)
        return request

    def _block_lane(self, lane, reason):
        previous = lane.get('blocked_reason')
        lane = self.state.update_lane(lane['id'], blocked_reason=reason)
        target = self._target_head(lane, block=False)
        for request in self.state.list_requests(
            lane['id'], include_terminal=False):
            if previous != reason:
                self._parent_log(request, 'waiting: {}'.format(reason))
            try:
                write_status(request, lane, target_head=target)
            except OSError:
                pass

    def _project_active(self):
        for request in self.state.list_requests(include_terminal=False):
            lane = self.state.get_lane(request['lane_id'])
            try:
                write_status(
                    request, lane,
                    target_head=self._target_head(lane, block=False))
            except OSError:
                pass

    def _recover_terminal_sidecars(self):
        """Project durable terminal DB states after a controller crash."""
        for request in self.state.list_requests(statuses={'landed', 'failed'}):
            if not Path(request['job_dir']).exists():
                continue
            if not request_owns_job_dir(request):
                continue
            payload = read_json(request['result_file'])
            expected = 'success' if request['status'] == 'landed' else 'failed'
            if (
                isinstance(payload, dict)
                and payload.get('status') == expected
                and sidecar_owns_request(request, payload)
            ):
                continue
            lane = self.state.get_lane(request['lane_id'])
            target = self._target_head(lane, block=False)
            try:
                if request['status'] == 'landed':
                    write_terminal(
                        request, lane, True, target_head=target,
                        recovered=True, command_exitcode=0,
                    )
                    self._parent_log(
                        request, 'recovered completed landing metadata')
                else:
                    write_terminal(
                        request, lane, False, target_head=target,
                        error=request.get('error') or 'merge failed',
                        recovered=True, command_exitcode=0,
                    )
                    self._parent_log(
                        request, 'recovered failed merge metadata')
            except OSError:
                # Keep the controller registration alive and retry; the DB is
                # the journal, while the sidecar is the backend projection.
                continue

    def terminal_projections_complete(self):
        for request in self.state.list_requests(statuses={'landed', 'failed'}):
            if not Path(request['job_dir']).exists():
                continue
            ownership = request_job_ownership(request)
            if ownership is False:
                # A later submission now owns this reused numeric job path.
                continue
            if ownership is None:
                return False
            payload = read_json(request['result_file'])
            expected = 'success' if request['status'] == 'landed' else 'failed'
            if (
                not isinstance(payload, dict)
                or payload.get('status') != expected
                or not sidecar_owns_request(request, payload)
            ):
                return False
        return True

    def _cleanup_lane_if_idle(self, lane):
        cleanup_lane_if_idle(self.state, lane)

    def _cleanup_idle_lanes(self):
        for lane in self.state.list_lanes():
            self._cleanup_lane_if_idle(lane)

    @staticmethod
    def _parent_log(request, message):
        if not request_owns_job_dir(request):
            return
        parent = read_json(request.get('meta_file')) or {}
        output = parent.get('output_file')
        if not output:
            return
        try:
            Path(output).parent.mkdir(parents=True, exist_ok=True)
            with open(output, 'a', encoding='utf-8') as stream:
                stream.write('[taskq] merge: {}\n'.format(message))
        except OSError:
            pass


def _unregister_controllers(state):
    for spec in state.backend_specs():
        try:
            if spec.get('name') != 'tmux':
                continue
            backend = TmuxBackend('tmux', _plain(spec['config']))
            backend.unregister_controller(spec['controller_name'])
        except (BackendError, OSError):
            continue


def run(state_path, interval=1.0):
    interval = max(0.1, float(interval))
    state = MergeState(state_path)
    controller = MergeController(state)
    try:
        while True:
            controller.reconcile()
            if state.active() == 0 and controller.terminal_projections_complete():
                # Close the enqueue-vs-unregister race with a quiet interval.
                time.sleep(interval)
                if (
                    state.active() == 0
                    and controller.terminal_projections_complete()
                ):
                    break
            else:
                time.sleep(interval)
    finally:
        lock_path = state.path.parent / 'controller.lock'
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, 'a', encoding='utf-8') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if (
                state.active() == 0
                and controller.terminal_projections_complete()
            ):
                _unregister_controllers(state)
        state.close()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', required=True)
    parser.add_argument('--interval', type=float, default=1.0)
    args = parser.parse_args(argv)
    run(args.state, args.interval)


if __name__ == '__main__':
    main()

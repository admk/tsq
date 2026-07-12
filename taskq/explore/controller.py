import argparse
import datetime
import fcntl
import json
import os
import shlex
import sqlite3
import sys
import time
from pathlib import Path

from ..backends import git_ref as git_ref_utils
from ..backends.base import BackendError
from ..backends.tmux.backend import TmuxBackend
from .agent import (
    AgentResponseError,
    build_optimizer_prompt,
    build_planner_prompt,
    build_rebase_prompt,
    build_reviewer_prompt,
    fingerprint_direction,
    is_stalled_output,
    parse_planner_response,
    parse_reviewer_response,
    render_command,
)
from .git import (
    changed_paths,
    diff,
    git,
    merge_ff,
    protected_paths,
    rebase,
    rebase_in_progress,
    require_clean,
    snapshot,
)
from .state import ExploreState
from .validation import MARKER as VALIDATION_MARKER
from .validation import eligible


TERMINAL = {'success', 'failed', 'killed', 'interrupted'}
VALIDATION_SCRIPT = str(Path(__file__).with_name('validation.py'))
AGENT_ROLES = {
    'planner', 'optimizer', 'adjust', 'reviewer',
    'rebase', 'landing_rebase', 'landing_reviewer',
}


def _plain(value):
    return json.loads(json.dumps(value))


def _tail_marker(output, marker):
    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            return json.loads(line[len(marker):])
    raise ValueError('missing {}'.format(marker.rstrip(':')))


class ExploreController:
    """One idempotent reconciliation loop for an exploration campaign."""

    def __init__(self, state, backend, campaign_id):
        self.state = state
        self.backend = backend
        self.campaign_id = campaign_id
        self.worker = 'controller-{}'.format(os.getpid())
        self._current_event_id = None

    @property
    def campaign(self):
        campaign = self.state.get_campaign(self.campaign_id)
        if campaign is None:
            raise KeyError(self.campaign_id)
        return campaign

    @property
    def config(self):
        return self.campaign['config']

    @property
    def budgets(self):
        return self.campaign['budgets']

    def reconcile(self):
        lock_path = Path(self.config['work_root']) / 'controller.lock'
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, 'a', encoding='utf-8') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            return self._reconcile_locked()

    def _reconcile_locked(self):
        campaign = self.campaign
        heartbeat = Path(self.config['heartbeat_file'])
        heartbeat.parent.mkdir(parents=True, exist_ok=True)
        heartbeat.touch()
        self.state.heartbeat(self.campaign_id, self.worker)
        if campaign['status'] == 'paused':
            return
        self._adopt_orphan_jobs()
        self._reconcile_attempt_counters()
        self._refresh_jobs()
        if (self.campaign['status'] == 'active' and
                time.time() >= float(self.budgets['deadline'])):
            self.state.update_campaign(self.campaign_id, status='draining')
        self._cleanup_finished_attempts()
        event = self.state.claim_event(self.worker, self.campaign_id)
        if event:
            self._handle_event(event)
            return
        if self._active_job('reviewer') or self._active_job('landing_reviewer'):
            return
        if not self.state.merge_queue_empty(self.campaign_id):
            self._advance_merge()
            return
        campaign = self.campaign
        if campaign['status'] in {'draining', 'landing', 'waiting_to_land'}:
            self._advance_landing()
            return
        if campaign['status'] != 'active':
            return
        if self._budget_exhausted():
            self.state.update_campaign(self.campaign_id, status='draining')
            return
        self._allocate()

    def _refresh_jobs(self):
        jobs = [
            job for job in self.state.list_jobs(campaign_id=self.campaign_id)
            if job['status'] not in TERMINAL
        ]
        if not jobs:
            return
        ids = [int(job['backend_job_id']) for job in jobs]
        current = {item['id']: item for item in self.backend.full_info(ids)}
        for job in jobs:
            info = current.get(int(job['backend_job_id']))
            status = info['status'] if info else 'interrupted'
            deadline_cancel = (
                info and status == 'queued' and self._speculative(job) and
                time.time() >= float(self.budgets['deadline']))
            if (deadline_cancel or (
                    info and status == 'running' and self._overdue(info, job))):
                if hasattr(self.backend, 'kill'):
                    self.backend.kill(info)
                status = 'killed'
            if status == job['status']:
                continue
            if status in TERMINAL:
                self.state.add_terminal_event(
                    job['id'], status, {'exit_code': info.get('exitcode') if info else None})
            else:
                self.state.update_job(job['id'], status=status)

    @staticmethod
    def _speculative(job):
        if job['role'] in {'planner', 'optimizer', 'adjust'}:
            return True
        return (job['role'] == 'validation' and
                job['metadata'].get('phase') in {'baseline', 'inspection'})

    def _adopt_orphan_jobs(self):
        for item in self.backend.full_info(None):
            backend_meta = item.get('metadata') or {}
            if backend_meta.get('campaign_id') != self.campaign_id:
                continue
            job_id = '{}:{}'.format(self.campaign_id, item['id'])
            if self.state.get_job(job_id):
                continue
            try:
                self.state.add_job(
                    self.campaign_id, job_id, backend_meta['role'], item['id'],
                    backend_meta.get('attempt_id'),
                    backend_meta.get('direction_id'), status=item['status'],
                    metadata=backend_meta.get('workflow_metadata') or {},
                    terminal_payload={
                        'exit_code': item.get('exitcode'), 'adopted': True,
                    } if item['status'] in TERMINAL else None)
            except Exception:
                self.backend.remove({'id': item['id']})
                raise

    def _overdue(self, info, job):
        started = info.get('start_time')
        if not started:
            return False
        if isinstance(started, str):
            started = datetime.datetime.fromisoformat(started)
        now = datetime.datetime.now(started.tzinfo) if started.tzinfo else datetime.datetime.now()
        return (now - started).total_seconds() > float(
            self.config.get('action_timeout', 1800))

    def _handle_event(self, event):
        job = self.state.get_job(event['job_id'])
        child_exists = any(
            child['metadata'].get('source_event_id') == event['id']
            for child in self.state.list_jobs(campaign_id=self.campaign_id))
        if child_exists:
            self.state.complete_event(event['id'])
            self._cleanup_event_worktrees(job)
            self._cleanup_finished_attempts()
            return
        self._current_event_id = event['id']
        try:
            output = self.backend.output(
                {'id': int(job['backend_job_id'])}, 0) or ''
            role = job['role']
            if role == 'planner':
                self._finish_planner(job, output)
            elif role in {'optimizer', 'adjust'}:
                self._finish_mutation(job, output)
            elif role == 'validation':
                self._finish_validation(job, output)
            elif role in {'reviewer', 'landing_reviewer'}:
                self._finish_review(job, output, event)
            elif role in {'rebase', 'landing_rebase'}:
                self._finish_rebase(job, output)
            self.state.complete_event(event['id'])
            self._cleanup_event_worktrees(job)
            self._cleanup_finished_attempts()
        except Exception as error:
            current_event = self.state.get_event(event['id'])
            payload = dict(current_event.get('payload') or {})
            retries = int(payload.get('controller_retries', 0)) + 1
            payload['controller_retries'] = retries
            if retries < 3:
                self.state.update_event(event['id'], payload=payload)
                self.state.release_event(event['id'], error=str(error))
            else:
                self.state.complete_event(
                    event['id'], status='failed', error=str(error))
                job = self.state.get_job(event['job_id'])
                request_id = (job.get('metadata') or {}).get('merge_request_id')
                if request_id:
                    request = self.state.get_merge_request(request_id)
                    if request and request['status'] in {'queued', 'processing'}:
                        self.state.complete_merge_request(
                            request_id, 'failed',
                            {'reason': 'controller event failed repeatedly'})
                if job.get('attempt_id'):
                    attempt = self.state.get_attempt(job['attempt_id'])
                    if attempt and attempt['status'] not in {'merged', 'abandoned'}:
                        self._abandon(attempt, 'controller event failed repeatedly')
                self._cleanup_event_worktrees(job)
                self._cleanup_finished_attempts()
            self.state.emit(
                self.campaign_id, 'controller.event_failed',
                {'event_id': event['id'], 'error': str(error)},
            )
        finally:
            self._current_event_id = None

    def _finish_planner(self, job, output):
        if not self._restore_control_worktree(job):
            self._stall_campaign('planner modified the campaign mainline')
            return
        if self.campaign['status'] != 'active':
            return
        count = int(job['metadata'].get('direction_count', 1))
        try:
            directions = parse_planner_response(output, count)
        except AgentResponseError as error:
            if self._queue_response_repair(job, error):
                return
            self._stall_campaign('invalid planner response: {}'.format(error))
            return
        campaign = self.campaign
        work_root = Path(self.config['work_root']) / 'attempts'
        work_root.mkdir(parents=True, exist_ok=True)
        existing = len(self.state.list_directions(self.campaign_id))
        blocked = (
            campaign['status'] != 'active' or
            not self.state.merge_queue_empty(self.campaign_id))
        for offset, value in enumerate(directions, 1):
            direction_id = '{}-d{:03d}'.format(self.campaign_id, existing + offset)
            fingerprint = fingerprint_direction(value)
            try:
                self.state.add_direction(
                    self.campaign_id, direction_id,
                    value.get('hypothesis') or value.get('title'), fingerprint,
                    generation=campaign['generation'],
                    status='deferred' if blocked else 'planned', metadata=value,
                )
            except sqlite3.IntegrityError:
                self._stall_campaign('planner repeated a tried direction')
                continue
            if not blocked:
                self._start_attempt(direction_id, work_root / direction_id)

    def _start_attempt(self, direction_id, worktree):
        if self._free_slots() < 1 or self._remaining_agent_jobs() < 4:
            return
        campaign = self.campaign
        attempt_id = '{}-a'.format(direction_id)
        branch = 'tq/explore/{}/attempt/{}'.format(
            self.campaign_id, direction_id.rsplit('-', 1)[-1])
        meta = git_ref_utils.create_branch_worktree(
            self.config['repo_root'], branch, worktree,
            campaign['mainline_head'],
        )
        self.state.add_attempt(
            self.campaign_id, attempt_id, direction_id, branch,
            meta['git_worktree'], campaign['mainline_head'],
            metadata={'workspace': meta},
        )
        direction = self.state.get_direction(direction_id)
        prompt = build_optimizer_prompt(
            campaign['objective'], direction['metadata'],
            memory=self._memory(), artifacts={},
            max_files=self.config['max_files'],
            max_lines=self.config['max_lines'],
        )
        if not self._queue_agent(
            'optimizer', prompt, worktree, attempt_id, direction_id
        ):
            self._abandon(
                self.state.get_attempt(attempt_id), 'agent budget exhausted')
            return
        self.state.update_direction(direction_id, status='running')

    def _finish_mutation(self, job, output):
        attempt = self.state.get_attempt(job['attempt_id'])
        event = (
            self.state.get_event(self._current_event_id)
            if self._current_event_id is not None else None)
        saved = (event.get('payload') or {}).get('mutation_artifacts') if event else None
        if saved is not None:
            self._dispatch_mutation_review(attempt, saved)
            return
        paths = changed_paths(attempt['worktree'], attempt['head'])
        protected = protected_paths(paths, self.config['protected_paths'])
        head, _ = snapshot(
            attempt['worktree'],
            'tq explore {} {}'.format(self.campaign_id, job['role']),
        )
        # The prior controller may have committed before persisting the event.
        changed = bool(paths)
        stale = is_stalled_output(output, changed=changed)
        updates = {'head': head, 'status': 'reviewing'}
        if stale:
            updates['stale_count'] = attempt['stale_count'] + 1
        action_diff = diff(attempt['worktree'], attempt['head'], head)
        changed_lines = sum(
            line.startswith(('+', '-')) and not line.startswith(('+++', '---'))
            for line in action_diff.splitlines())
        limit_violation = (
            len(paths) > int(self.config['max_files']) or
            changed_lines > int(self.config['max_lines']))
        artifacts = {
            'job_status': job['status'],
            'worker_output': output[-20000:],
            'changed_paths': paths,
            'protected_paths': protected,
            'diff': diff(attempt['worktree'], attempt['base_head'], head),
            'stalled': stale,
            'changed': changed,
            'stale_count': updates.get('stale_count', attempt['stale_count']),
            'changed_lines': changed_lines,
            'limit_violation': limit_violation,
        }
        if event:
            attempt = self.state.record_mutation_event(
                event['id'], attempt['id'], head,
                updates.get('stale_count', attempt['stale_count']), artifacts)
        else:
            attempt = self.state.update_attempt(attempt['id'], **updates)
        self._dispatch_mutation_review(attempt, artifacts)

    def _dispatch_mutation_review(self, attempt, artifacts):
        if self._has_validation():
            self._queue_validation(attempt, 'inspection', artifacts)
        else:
            self._queue_reviewer(attempt, 'inspection', artifacts)

    def _finish_validation(self, job, output):
        attempt = self.state.get_attempt(job['attempt_id']) if job['attempt_id'] else None
        metadata = job['metadata']
        try:
            result = _tail_marker(output, VALIDATION_MARKER)
        except (ValueError, json.JSONDecodeError) as error:
            result = {'checks_passed': False, 'score_error': str(error)}
        if not self._restore_validation_worktree(job):
            result = {
                'checks_passed': False,
                'score_error': 'validation modified its worktree',
            }
        if metadata.get('phase') == 'baseline':
            if self.campaign['status'] == 'draining':
                return
            config = dict(self.config)
            config['baseline_validation'] = result
            if not result.get('checks_passed') or result.get('score_error'):
                self.state.update_campaign(
                    self.campaign_id, config=config, status='failed')
            else:
                self.state.update_campaign(self.campaign_id, config=config)
            return
        if metadata.get('phase') == 'landing_validation':
            artifacts = dict(metadata.get('artifacts') or {})
            artifacts['validation'] = result
            self._queue_landing_reviewer(artifacts)
            return
        artifacts = dict(metadata.get('artifacts') or {})
        artifacts['validation'] = result
        self._queue_reviewer(attempt, metadata['phase'], artifacts,
                             metadata.get('merge_request_id'))

    def _queue_validation(self, attempt, phase, artifacts, merge_request_id=None):
        cwd = self._validation_worktree(attempt['head'], attempt['id'])
        baseline_head = self.campaign['mainline_head']
        baseline_cwd = self._validation_worktree(
            baseline_head, '{}-baseline'.format(attempt['id']))
        spec_path = Path(self.config['work_root']) / 'artifacts' / (
            '{}-{}-validation.json'.format(attempt['id'], time.time_ns()))
        spec_path.parent.mkdir(parents=True, exist_ok=True)
        spec_path.write_text(json.dumps({
            'checks': self.config.get('checks', []),
            'score': self.config.get('score'),
            'score_repeats': 3,
            'score_seed': time.time_ns(),
            'baseline_cwd': str(baseline_cwd),
        }), encoding='utf-8')
        command = [sys.executable, '-I', VALIDATION_SCRIPT,
                   '--spec', str(spec_path)]
        try:
            self._queue_job(
                'validation', command, cwd, attempt['id'],
                attempt['direction_id'], metadata={
                    'phase': phase, 'artifacts': artifacts,
                    'merge_request_id': merge_request_id,
                    'validation_cwd': str(cwd),
                    'expected_head': attempt['head'],
                    'validation_worktrees': [
                        {'cwd': str(cwd), 'head': attempt['head']},
                        {'cwd': str(baseline_cwd), 'head': baseline_head},
                    ],
                },
            )
        except Exception:
            self._remove_validation_worktree(cwd)
            self._remove_validation_worktree(baseline_cwd)
            raise

    def _queue_reviewer(
        self, attempt, phase, artifacts, merge_request_id=None,
    ):
        baseline = self.config.get('baseline_validation') or {}
        validation = artifacts.get('validation') or {}
        allowed = (
            not artifacts.get('protected_paths') and
            artifacts.get('changed', True) and
            not artifacts.get('limit_violation') and eligible(
            validation or {'checks_passed': True},
            validation.get('baseline_score', baseline.get('score')),
            self.config.get('score_direction'),
            self.config.get('min_improvement', 0),
            ))
        artifacts['eligible'] = allowed
        direction = self.state.get_direction(attempt['direction_id'])
        prompt = build_reviewer_prompt(
            self.campaign['objective'], direction['metadata'],
            memory=self._memory(), artifacts=artifacts,
            phase=phase, max_files=self.config['max_files'],
            max_lines=self.config['max_lines'],
        )
        job_id = self._queue_agent(
            'reviewer', prompt, self.config['control_cwd'],
            attempt['id'], attempt['direction_id'], control=True,
            metadata={
                'phase': phase, 'artifacts': artifacts,
                'merge_request_id': merge_request_id,
            },
        )
        if job_id is None:
            if phase == 'merge' and merge_request_id is not None:
                self.state.complete_merge_request(
                    merge_request_id, 'failed',
                    {'reason': 'agent budget exhausted before review'})
            self._abandon(attempt, 'agent budget exhausted before review')

    def _finish_review(self, job, output, event):
        metadata = job['metadata']
        attempt = self.state.get_attempt(job['attempt_id']) if job['attempt_id'] else None
        if not self._restore_control_worktree(job):
            output = ''
        try:
            decision = parse_reviewer_response(output)
        except AgentResponseError as error:
            if self._queue_response_repair(job, error):
                return
            decision = {
                'decision': 'abandon',
                'reason': 'invalid reviewer response: {}'.format(error),
                'evidence': [], 'memory_updates': [], 'next_direction': None,
            }
        if decision['decision'] == 'accept' and not metadata['artifacts'].get('eligible'):
            decision['decision'] = 'adjust'
            decision['reason'] = 'candidate failed a system-controlled acceptance gate'
        phase = metadata.get('phase', 'inspection')
        merge_request_id = metadata.get('merge_request_id')
        self.state.add_decision(
            self.campaign_id, decision['decision'],
            attempt_id=attempt['id'] if attempt else None,
            event_id=event['id'], merge_request_id=merge_request_id,
            phase=phase, generation=self.campaign['generation'],
            reason=decision['reason'], evidence=decision['evidence'],
            memory_updates=decision['memory_updates'],
            next_direction=decision['next_direction'],
        )
        self._record_memory(attempt, decision, phase)
        if phase == 'landing':
            self._apply_landing_decision(decision, metadata)
        elif phase == 'merge':
            self._apply_merge_decision(attempt, decision, merge_request_id, metadata)
        else:
            self._apply_inspection_decision(attempt, decision)

    def _apply_inspection_decision(self, attempt, decision):
        attempt = self.state.get_attempt(attempt['id'])
        if attempt['status'] not in {'active', 'reviewing'}:
            return
        value = decision['decision']
        if value == 'accept':
            merged = self.state.list_merge_requests(self.campaign_id, status='merged')
            if len(merged) >= int(self.budgets['max_merges']):
                self._abandon(attempt, 'merge limit reached')
                self.state.update_campaign(self.campaign_id, status='draining')
                return
            self.state.update_attempt(attempt['id'], status='merge_queued')
            self.state.enqueue_merge_request(
                self.campaign_id, attempt['id'], attempt['head'])
        elif value == 'adjust':
            if self.campaign['status'] != 'active':
                self._abandon(attempt, 'campaign is draining')
            elif attempt['stale_count'] >= 2:
                self._abandon(
                    attempt, 'repeated stale actions require a structural pivot')
            elif attempt['adjustments'] >= int(self.budgets['max_adjustments']):
                self._abandon(attempt, 'adjustment limit reached')
            elif self.state.merge_queue_empty(self.campaign_id) and self._free_slots() > 0:
                self._queue_adjustment(attempt, decision)
            else:
                self.state.update_attempt(attempt['id'], status='deferred')
        elif (value == 'evaluate_more' and self._has_validation() and
              self.campaign['status'] == 'active'):
            self._queue_validation(attempt, 'inspection', {'reevaluation': True})
        elif value == 'stop':
            self.state.update_campaign(self.campaign_id, status='draining')
            self.state.update_attempt(attempt['id'], status='stopped')
        else:
            self._abandon(attempt, decision['reason'])

    def _queue_adjustment(self, attempt, decision):
        if self._remaining_agent_jobs() < 4:
            self._abandon(attempt, 'agent budget exhausted before adjustment')
            return
        direction = self.state.get_direction(attempt['direction_id'])
        prompt = build_optimizer_prompt(
            self.campaign['objective'], direction['metadata'], adjust=True,
            memory=self._memory(), artifacts={'review': decision},
            max_files=self.config['max_files'], max_lines=self.config['max_lines'],
        )
        number = attempt['adjustments'] + 1
        job_id = self._queue_agent(
            'adjust', prompt, attempt['worktree'], attempt['id'],
            attempt['direction_id'], metadata={'adjustment_number': number})
        if job_id:
            self.state.update_attempt(
                attempt['id'], adjustments=number, status='active')

    def _reconcile_attempt_counters(self):
        numbers = {}
        for job in self.state.list_jobs(
            campaign_id=self.campaign_id, role='adjust'):
            if job['attempt_id'] and job['metadata'].get('adjustment_number'):
                numbers[job['attempt_id']] = max(
                    numbers.get(job['attempt_id'], 0),
                    int(job['metadata']['adjustment_number']))
        for attempt_id, number in numbers.items():
            attempt = self.state.get_attempt(attempt_id)
            if attempt and attempt['adjustments'] < number:
                self.state.update_attempt(attempt_id, adjustments=number)

    def _advance_merge(self):
        merged = self.state.list_merge_requests(self.campaign_id, status='merged')
        if len(merged) >= int(self.budgets['max_merges']):
            for request in self.state.list_merge_requests(
                self.campaign_id, status='queued'):
                attempt = self.state.get_attempt(request['attempt_id'])
                self.state.complete_merge_request(
                    request['id'], 'cancelled', {'reason': 'merge limit reached'})
                self._abandon(attempt, 'merge limit reached')
            self.state.update_campaign(self.campaign_id, status='draining')
            return
        processing = self.state.list_merge_requests(
            self.campaign_id, status='processing')
        if processing:
            request = processing[0]
            waiting = any(
                job['status'] not in TERMINAL and
                job['metadata'].get('merge_request_id') == request['id']
                for job in self.state.list_jobs(campaign_id=self.campaign_id)
            )
            if not waiting:
                metadata = request.get('metadata') or {}
                expected = metadata.get('expected_head')
                actual = git(self.config['mainline_worktree'], 'rev-parse', 'HEAD')
                if metadata.get('stage') == 'merging' and expected == actual:
                    attempt = self.state.get_attempt(request['attempt_id'])
                    self._record_merged(request, attempt, actual, metadata)
                else:
                    self.state.release_merge_request(
                        request['id'], {'reason': 'recovered unfinished merge step'})
            return
        request = self.state.claim_merge_request(self.campaign_id, self.worker)
        if not request:
            return
        attempt = self.state.get_attempt(request['attempt_id'])
        target = self.campaign['mainline_head']
        ok, output = rebase(attempt['worktree'], target)
        if not ok:
            if self._remaining_agent_jobs() < 3:
                self.state.complete_merge_request(
                    request['id'], 'rejected',
                    {'reason': 'agent budget cannot cover conflict and final reviews'})
                self._abandon(
                    attempt, 'agent budget cannot cover conflict and final reviews')
                return
            prompt = build_rebase_prompt(
                self.campaign['objective'],
                self.state.get_direction(attempt['direction_id'])['metadata'],
                memory=self._memory(), artifacts={'git': output},
                max_files=self.config['max_files'], max_lines=self.config['max_lines'],
            )
            self.state.update_merge_request(
                request['id'], metadata={'stage': 'resolving'})
            job_id = self._queue_agent(
                'rebase', prompt, attempt['worktree'], attempt['id'],
                attempt['direction_id'], metadata={'merge_request_id': request['id']})
            if job_id is None:
                self.state.complete_merge_request(
                    request['id'], 'rejected',
                    {'reason': 'agent budget exhausted before conflict resolution'})
                self._abandon(attempt, 'agent budget exhausted before conflict resolution')
            return
        head = git(attempt['worktree'], 'rev-parse', 'HEAD')
        attempt = self.state.update_attempt(attempt['id'], head=head)
        self.state.update_merge_request(
            request['id'], head=head, metadata={'stage': 'reviewing'})
        self._review_rebased(attempt, request['id'])

    def _finish_rebase(self, job, output):
        if job['role'] == 'landing_rebase':
            valid = job['status'] == 'success' and not rebase_in_progress(
                self.config['mainline_worktree'])
            if valid:
                target = git(
                    self.config['repo_root'], 'rev-parse',
                    self.campaign['target_ref'])
                valid = self._is_ancestor(
                    self.config['mainline_worktree'], target, 'HEAD')
            if not valid:
                self.state.update_campaign(self.campaign_id, status='landing_failed')
                return
            self._queue_landing_review()
            return
        request_id = job['metadata']['merge_request_id']
        attempt = self.state.get_attempt(job['attempt_id'])
        valid = job['status'] == 'success' and not rebase_in_progress(
            attempt['worktree']) and self._is_ancestor(
                attempt['worktree'], self.campaign['mainline_head'], 'HEAD')
        if not valid:
            self.state.complete_merge_request(
                request_id, 'rejected', {'reason': 'rebase conflict unresolved'})
            self._abandon(attempt, 'rebase conflict unresolved')
            return
        head = git(attempt['worktree'], 'rev-parse', 'HEAD')
        attempt = self.state.update_attempt(attempt['id'], head=head)
        self.state.update_merge_request(request_id, head=head)
        self._review_rebased(attempt, request_id)

    def _review_rebased(self, attempt, request_id):
        artifacts = {
            'diff': diff(
                attempt['worktree'], self.campaign['mainline_head'], attempt['head']),
            'changed_paths': changed_paths(
                attempt['worktree'], self.campaign['mainline_head']),
        }
        artifacts['protected_paths'] = protected_paths(
            artifacts['changed_paths'], self.config['protected_paths'])
        if self._has_validation():
            self._queue_validation(attempt, 'merge', artifacts, request_id)
        else:
            self._queue_reviewer(attempt, 'merge', artifacts, request_id)

    def _apply_merge_decision(self, attempt, decision, request_id, metadata):
        request = self.state.get_merge_request(request_id)
        if request is None or request['status'] not in {'queued', 'processing'}:
            return
        if decision['decision'] != 'accept':
            status = 'deferred' if decision['decision'] == 'adjust' else 'rejected'
            self.state.complete_merge_request(
                request_id, status, {'reason': decision['reason']})
            self.state.update_attempt(attempt['id'], status=status)
            if status == 'rejected':
                self._abandon(attempt, decision['reason'])
            if decision['decision'] == 'stop':
                self.state.update_campaign(self.campaign_id, status='draining')
            return
        campaign = self.campaign
        current = git(self.config['mainline_worktree'], 'rev-parse', 'HEAD')
        if current != campaign['mainline_head']:
            self.state.release_merge_request(request_id, {'reason': 'mainline moved'})
            return
        branch_head = git(attempt['worktree'], 'rev-parse', 'HEAD')
        if branch_head != request['head'] or git(
            attempt['worktree'], 'status', '--porcelain',
            '--untracked-files=normal'):
            self.state.complete_merge_request(
                request_id, 'failed', {'reason': 'candidate changed after review'})
            self._abandon(attempt, 'candidate changed after review')
            return
        request = self.state.update_merge_request(request_id, metadata={
            'stage': 'merging', 'expected_head': request['head'],
            'review_artifacts': metadata.get('artifacts', {}),
            'reason': decision['reason'],
        })
        head = merge_ff(self.config['mainline_worktree'], attempt['branch'])
        self._record_merged(request, attempt, head, request['metadata'])

    def _record_merged(self, request, attempt, head, metadata):
        config = self._promote_baseline({
            'artifacts': metadata.get('review_artifacts', {})})
        self.state.finalize_merge_request(
            request['id'], head, campaign_config=config, result={'head': head})
        self.state.add_finding(
            metadata.get('reason') or 'accepted optimization', 'confirmed',
            campaign_id=self.campaign_id,
            attempt_id=attempt['id'], direction_id=attempt['direction_id'],
            outcome='merged', source_commit=head,
            provenance={'merge_request_id': request['id']},
            dedupe_key='merged:{}:{}'.format(self.campaign_id, head),
        )

    def _allocate(self):
        for attempt in self.state.list_attempts(
            self.campaign_id, status='deferred'):
            self._abandon(
                attempt, 'deferred work was superseded by the merged mainline')
        active = sum(
            item['status'] not in {
                'merged', 'abandoned', 'rejected', 'stopped', 'deferred'}
            for item in self.state.list_attempts(self.campaign_id)
        )
        free = min(
            self._free_slots(), max(0, int(self.budgets['parallel']) - active),
            max(0, (self._remaining_agent_jobs() - 1) // 3))
        if free <= 0:
            if self._remaining_agent_jobs() < 5 and not self._active_mutation_jobs():
                self.state.update_campaign(self.campaign_id, status='draining')
            return
        planned = self.state.list_directions(self.campaign_id, status='planned')
        for direction in planned[:free]:
            path = Path(self.config['work_root']) / 'attempts' / direction['id']
            self._start_attempt(direction['id'], path)
        active = sum(
            item['status'] not in {
                'merged', 'abandoned', 'rejected', 'stopped', 'deferred'}
            for item in self.state.list_attempts(self.campaign_id)
        )
        free = min(
            self._free_slots(), max(0, int(self.budgets['parallel']) - active),
            max(0, (self._remaining_agent_jobs() - 2) // 3))
        if free <= 0 or self._active_job('planner'):
            return
        if self._has_validation() and 'baseline_validation' not in self.config:
            self._queue_baseline()
            return
        prompt = build_planner_prompt(
            self.campaign['objective'], memory=self._memory(),
            tried_directions=self.state.list_directions(self.campaign_id),
            direction_count=free, max_files=self.config['max_files'],
            max_lines=self.config['max_lines'],
        )
        job_id = self._queue_agent(
            'planner', prompt, self.config['control_cwd'], control=True,
            metadata={'direction_count': free})
        if job_id is None:
            self.state.update_campaign(self.campaign_id, status='draining')

    def _queue_baseline(self):
        spec_path = Path(self.config['work_root']) / 'artifacts' / 'baseline.json'
        spec_path.parent.mkdir(parents=True, exist_ok=True)
        spec_path.write_text(json.dumps({
            'checks': self.config.get('checks', []),
            'score': self.config.get('score'), 'score_repeats': 3,
        }), encoding='utf-8')
        head = self.campaign['mainline_head']
        cwd = self._validation_worktree(head, 'baseline')
        try:
            self._queue_job(
                'validation', [sys.executable, '-I', VALIDATION_SCRIPT,
                               '--spec', str(spec_path)], cwd,
                metadata={
                    'phase': 'baseline', 'artifacts': {},
                    'validation_cwd': str(cwd), 'expected_head': head,
                },
            )
        except Exception:
            self._remove_validation_worktree(cwd)
            raise

    def _advance_landing(self):
        campaign = self.campaign
        if self._active_mutation_jobs() or not self.state.merge_queue_empty(self.campaign_id):
            return
        if self._active_job('landing_rebase') or self._active_job('landing_reviewer'):
            return
        config = dict(self.config)
        stage = config.get('landing_stage')
        if stage == 'reviewed':
            self._land_target()
            return
        ok, output = rebase(self.config['mainline_worktree'], campaign['target_ref'])
        if not ok:
            if self._remaining_agent_jobs() < 2:
                self.state.update_campaign(
                    self.campaign_id, status='landing_failed')
                return
            prompt = build_rebase_prompt(
                campaign['objective'], {'hypothesis': 'land campaign mainline'},
                memory=self._memory(), artifacts={'git': output},
                max_files=self.config['max_files'], max_lines=self.config['max_lines'],
            )
            job_id = self._queue_agent(
                'landing_rebase', prompt, self.config['mainline_worktree'],
                control=False)
            if job_id is None:
                self.state.update_campaign(
                    self.campaign_id, status='landing_failed')
                return
            self.state.update_campaign(self.campaign_id, status='landing')
            return
        self._queue_landing_review()

    def _queue_landing_review(self):
        head = git(self.config['mainline_worktree'], 'rev-parse', 'HEAD')
        config = dict(self.config)
        config['landing_head'] = head
        self.state.update_campaign(
            self.campaign_id, mainline_head=head, status='landing', config=config)
        artifacts = {
            'diff': diff(self.config['repo_root'], self.campaign['target_ref'], head),
        }
        if self._has_validation():
            spec_path = Path(self.config['work_root']) / 'artifacts' / 'landing.json'
            spec_path.parent.mkdir(parents=True, exist_ok=True)
            spec_path.write_text(json.dumps({
                'checks': self.config.get('checks', []),
                'score': self.config.get('score'), 'score_repeats': 3,
            }), encoding='utf-8')
            cwd = self._validation_worktree(head, 'landing')
            target_head = git(
                self.config['repo_root'], 'rev-parse', self.campaign['target_ref'])
            baseline_cwd = self._validation_worktree(
                target_head, 'landing-baseline')
            spec = json.loads(spec_path.read_text(encoding='utf-8'))
            spec.update({
                'baseline_cwd': str(baseline_cwd),
                'score_seed': time.time_ns(),
            })
            spec_path.write_text(json.dumps(spec), encoding='utf-8')
            try:
                self._queue_job(
                    'validation', [sys.executable, '-I', VALIDATION_SCRIPT,
                                   '--spec', str(spec_path)], cwd,
                    metadata={
                        'phase': 'landing_validation', 'artifacts': artifacts,
                        'validation_cwd': str(cwd), 'expected_head': head,
                        'validation_worktrees': [
                            {'cwd': str(cwd), 'head': head},
                            {'cwd': str(baseline_cwd), 'head': target_head},
                        ],
                    })
            except Exception:
                self._remove_validation_worktree(cwd)
                self._remove_validation_worktree(baseline_cwd)
                raise
            return
        self._queue_landing_reviewer(artifacts)

    def _queue_landing_reviewer(self, artifacts):
        validation = artifacts.get('validation') or {'checks_passed': True}
        artifacts['eligible'] = eligible(
            validation, validation.get('baseline_score',
                (self.config.get('baseline_validation') or {}).get('score')),
            self.config.get('score_direction'),
            0,
        )
        prompt = build_reviewer_prompt(
            self.campaign['objective'], {'hypothesis': 'land campaign mainline'},
            memory=self._memory(), artifacts=artifacts,
            max_files=self.config['max_files'], max_lines=self.config['max_lines'],
        )
        job_id = self._queue_agent(
            'landing_reviewer', prompt, self.config['control_cwd'],
            control=True, metadata={'phase': 'landing', 'artifacts': artifacts})
        if job_id is None:
            self.state.update_campaign(self.campaign_id, status='landing_failed')

    def _apply_landing_decision(self, decision, metadata):
        if (self.campaign['status'] in {'completed', 'landing_failed'} or
                self.config.get('landing_stage') == 'reviewed'):
            return
        if decision['decision'] != 'accept':
            self.state.update_campaign(self.campaign_id, status='landing_failed')
            return
        config = dict(self.config)
        config['landing_stage'] = 'reviewed'
        self.state.update_campaign(self.campaign_id, config=config)

    def _land_target(self):
        try:
            require_clean(self.config['repo_root'])
        except BackendError:
            self.state.update_campaign(self.campaign_id, status='waiting_to_land')
            return
        root = self.config['repo_root']
        branch = git(root, 'symbolic-ref', '--short', 'HEAD')
        if branch != self.campaign['target_ref']:
            self.state.update_campaign(self.campaign_id, status='waiting_to_land')
            return
        target_head = git(root, 'rev-parse', 'HEAD')
        try:
            git(root, 'merge-base', '--is-ancestor', target_head,
                self.config['mainline_branch'])
        except BackendError:
            config = dict(self.config)
            config.pop('landing_stage', None)
            self.state.update_campaign(
                self.campaign_id, status='landing', config=config)
            return
        head = merge_ff(root, self.config['mainline_branch'])
        self.state.update_campaign(
            self.campaign_id, target_head=head, mainline_head=head,
            status='completed', finished_at=self._now())
        self._cleanup_mainline()

    def _queue_agent(
        self, role, prompt, cwd, attempt_id=None, direction_id=None,
        control=False, metadata=None,
    ):
        if self._agent_jobs() >= int(self.budgets['max_agent_jobs']):
            return None
        argv = render_command(self.config['command'], prompt)
        data = dict(metadata or {})
        data['agent'] = True
        if role in {'planner', 'reviewer', 'landing_reviewer'}:
            data['response_prompt'] = prompt
        if control:
            control_path = Path(self.config['control_cwd']) / '{}-{}'.format(
                role, time.time_ns())
            git_ref_utils.create_worktree(
                self.config['repo_root'], self.campaign['mainline_head'],
                control_path)
            cwd = control_path
            data['control_worktree'] = str(control_path)
            data['control_head'] = self.campaign['mainline_head']
        try:
            return self._queue_job(
                role, argv, cwd, attempt_id, direction_id,
                slots=0 if control else 1, internal=control, metadata=data)
        except Exception:
            if control:
                git_ref_utils.remove_worktree({
                    'git_root': self.config['repo_root'],
                    'git_worktree': str(cwd),
                }, force=True)
            raise

    def _queue_job(
        self, role, argv, cwd, attempt_id=None, direction_id=None,
        slots=1, internal=False, metadata=None,
    ):
        metadata = dict(metadata or {})
        if self._current_event_id is not None:
            metadata['source_event_id'] = self._current_event_id
        backend_id = self.backend.add(
            shlex.join([str(value) for value in argv]), gpus=0, slots=slots,
            cwd=str(cwd), internal=internal, workspace_owner='campaign',
            metadata={
                'campaign_id': self.campaign_id, 'attempt_id': attempt_id,
                'direction_id': direction_id, 'role': role,
                'workflow_metadata': metadata,
            },
        )
        job_id = '{}:{}'.format(self.campaign_id, backend_id)
        try:
            self.state.add_job(
                self.campaign_id, job_id, role, backend_id,
                attempt_id, direction_id, metadata=metadata)
        except Exception:
            self.backend.remove({'id': int(backend_id)})
            raise
        return job_id

    def _active_job(self, role):
        return any(job['status'] not in TERMINAL for job in
                   self.state.list_jobs(campaign_id=self.campaign_id, role=role))

    def _active_mutation_jobs(self):
        return any(
            job['status'] not in TERMINAL and job['role'] in
            {'optimizer', 'adjust', 'validation', 'rebase', 'landing_rebase'}
            for job in self.state.list_jobs(campaign_id=self.campaign_id)
        )

    def _free_slots(self):
        capacity = int(self.backend.config.get('slots', 1))
        info = self.backend.full_info(None)
        claimed = sum(
            int(item.get('slots_required', 1)) for item in info
            if item['status'] in {'queued', 'running'}
        )
        return max(0, capacity - claimed)

    def _agent_jobs(self):
        return sum(
            bool(job['metadata'].get('agent'))
            for job in self.state.list_jobs(campaign_id=self.campaign_id)
        )

    def _remaining_agent_jobs(self):
        return max(0, int(self.budgets['max_agent_jobs']) - self._agent_jobs())

    def _queue_response_repair(self, job, error):
        metadata = dict(job['metadata'])
        if metadata.get('repair_count', 0) >= 1:
            return False
        prompt = metadata.get('response_prompt')
        if not prompt:
            return False
        metadata['repair_count'] = 1
        prompt += (
            '\n\nYour previous response was invalid: {}. Return the required '
            'TASKQ_JSON line and no trailing text.'.format(error))
        return bool(self._queue_agent(
            job['role'], prompt, self.config['control_cwd'],
            job.get('attempt_id'), job.get('direction_id'), control=True,
            metadata=metadata))

    def _restore_control_worktree(self, job):
        cwd = job['metadata'].get('control_worktree')
        expected = job['metadata'].get('control_head')
        if not cwd:
            return True
        current = git(cwd, 'rev-parse', 'HEAD')
        dirty = bool(git(cwd, 'status', '--porcelain', '--untracked-files=normal'))
        return current == expected and not dirty

    def _restore_validation_worktree(self, job):
        metadata = job['metadata']
        worktrees = metadata.get('validation_worktrees')
        if worktrees:
            clean = True
            for item in worktrees:
                cwd, expected = item['cwd'], item['head']
                current = git(cwd, 'rev-parse', 'HEAD')
                dirty = bool(git(
                    cwd, 'status', '--porcelain', '--untracked-files=normal'))
                clean = clean and current == expected and not dirty
            return clean
        cwd = metadata.get('validation_cwd')
        expected = metadata.get('expected_head')
        if not cwd or not expected:
            return True
        current = git(cwd, 'rev-parse', 'HEAD')
        dirty = bool(git(cwd, 'status', '--porcelain', '--untracked-files=normal'))
        return current == expected and not dirty

    def _cleanup_event_worktrees(self, job):
        metadata = job.get('metadata') or {}
        paths = []
        if metadata.get('control_worktree'):
            paths.append(metadata['control_worktree'])
        paths.extend(
            item['cwd'] for item in metadata.get('validation_worktrees', []))
        if metadata.get('validation_cwd') and not metadata.get('validation_worktrees'):
            paths.append(metadata['validation_cwd'])
        for path in dict.fromkeys(paths):
            try:
                self._remove_validation_worktree(path)
            except BackendError as error:
                self.state.emit(
                    self.campaign_id, 'cleanup.failed',
                    {'job_id': job['id'], 'worktree': path, 'error': str(error)})

    def _validation_worktree(self, head, label):
        path = Path(self.config['work_root']) / 'validation' / '{}-{}'.format(
            label, time.time_ns())
        git_ref_utils.create_worktree(
            self.config['repo_root'], head, path)
        return path

    def _remove_validation_worktree(self, path):
        git_ref_utils.remove_worktree({
            'git_root': self.config['repo_root'],
            'git_worktree': str(path),
        }, force=True)

    @staticmethod
    def _is_ancestor(cwd, ancestor, descendant):
        try:
            git(cwd, 'merge-base', '--is-ancestor', ancestor, descendant)
            return True
        except BackendError:
            return False

    def _budget_exhausted(self):
        budgets = self.budgets
        if self._agent_jobs() >= int(budgets['max_agent_jobs']):
            return True
        merged = self.state.list_merge_requests(self.campaign_id, status='merged')
        if len(merged) >= int(budgets['max_merges']):
            return True
        return time.time() >= float(budgets['deadline'])

    def _has_validation(self):
        return bool(self.config.get('checks') or self.config.get('score'))

    def _memory(self):
        return {
            'findings': self.state.list_findings(limit=20),
            'directions': self.state.list_directions(self.campaign_id)[-20:],
            'decisions': self.state.list_decisions(self.campaign_id)[-20:],
        }

    def _record_memory(self, attempt, decision, phase):
        for index, value in enumerate(decision.get('memory_updates') or []):
            claim = value.get('claim') if isinstance(value, dict) else str(value)
            if not claim:
                continue
            self.state.add_finding(
                claim, 'interpreted', campaign_id=self.campaign_id,
                attempt_id=attempt['id'] if attempt else None,
                direction_id=attempt['direction_id'] if attempt else None,
                outcome=decision['decision'],
                provenance={'phase': phase, 'reviewer': True},
                dedupe_key='review:{}:{}:{}'.format(
                    self.campaign_id, attempt['id'] if attempt else 'landing', index),
            )

    def _promote_baseline(self, metadata):
        config = dict(self.config)
        validation = metadata.get('artifacts', {}).get('validation')
        if validation:
            config['baseline_validation'] = validation
        return config

    def _stall_campaign(self, reason):
        campaign = self.campaign
        count = campaign['stall_count'] + 1
        status = 'failed' if count >= 4 else campaign['status']
        self.state.update_campaign(
            self.campaign_id, stall_count=count, status=status)
        self.state.emit(self.campaign_id, 'campaign.stalled', {'reason': reason})

    def _abandon(self, attempt, reason):
        self.state.update_attempt(attempt['id'], status='abandoned')
        self.state.update_direction(attempt['direction_id'], status='abandoned')
        self.state.add_finding(
            reason, 'interpreted', campaign_id=self.campaign_id,
            attempt_id=attempt['id'], direction_id=attempt['direction_id'],
            outcome='abandoned', source_commit=attempt['head'],
            dedupe_key='abandon:{}'.format(attempt['id']),
        )

    def _cleanup_attempt(self, attempt):
        meta = dict(attempt['metadata'].get('workspace') or {})
        meta.update({
            'git_root': self.config['repo_root'],
            'git_worktree': attempt['worktree'],
            'git_branch': attempt['branch'],
            'workspace_owner': 'campaign',
        })
        try:
            git_ref_utils.remove_branch_worktree(
                meta, delete_branch=True, force_branch=True)
        except BackendError as error:
            self.state.emit(
                self.campaign_id, 'cleanup.failed',
                {'attempt_id': attempt['id'], 'error': str(error)})

    def _cleanup_finished_attempts(self):
        active = {
            job['attempt_id'] for job in
            self.state.list_jobs(campaign_id=self.campaign_id)
            if job['attempt_id'] and job['status'] not in TERMINAL
        }
        pending_jobs = {
            event['job_id'] for event in self.state.list_events(self.campaign_id)
            if event['status'] in {'pending', 'processing'}
        }
        active.update(
            job['attempt_id'] for job in
            self.state.list_jobs(campaign_id=self.campaign_id)
            if job['id'] in pending_jobs and job['attempt_id'])
        for status in ('merged', 'abandoned'):
            for attempt in self.state.list_attempts(
                self.campaign_id, status=status):
                if attempt['id'] not in active and Path(attempt['worktree']).exists():
                    self._cleanup_attempt(attempt)

    def _cleanup_mainline(self):
        meta = {
            'git_root': self.config['repo_root'],
            'git_worktree': self.config['mainline_worktree'],
            'git_branch': self.config['mainline_branch'],
            'workspace_owner': 'campaign',
        }
        try:
            git_ref_utils.remove_branch_worktree(
                meta, delete_branch=True, force_branch=True)
        finally:
            self.backend.unregister_controller(self.campaign_id)

    @staticmethod
    def _now():
        import datetime
        return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run(state_path, campaign_id):
    state = ExploreState(state_path)
    campaign = state.get_campaign(campaign_id)
    backend = TmuxBackend('tmux', _plain(campaign['config']['backend_config']))
    controller = ExploreController(state, backend, campaign_id)
    interval = float(campaign['config'].get('controller_interval', 1))
    try:
        while controller.campaign['status'] not in {
            'completed', 'failed', 'landing_failed',
        }:
            controller.reconcile()
            time.sleep(interval)
    finally:
        if state.get_campaign(campaign_id)['status'] in {
            'completed', 'failed', 'landing_failed',
        }:
            backend.unregister_controller(campaign_id)
        state.close()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', required=True)
    parser.add_argument('--campaign', required=True)
    args = parser.parse_args(argv)
    run(args.state, args.campaign)


if __name__ == '__main__':
    main()

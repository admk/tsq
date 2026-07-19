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
from ..integration import fast_forward_checked_out, target_integration_lock
from .agent import (
    AgentResponseError,
    build_fix_prompt,
    build_merge_prompt,
    build_optimizer_prompt,
    build_planner_prompt,
    fingerprint_direction,
    is_stalled_output,
    parse_fix_response,
    parse_planner_response,
    render_command,
    render_prompt,
)
from .git import (
    abort_rebase,
    begin_no_ff_merge,
    changed_line_count,
    changed_paths,
    checkout_detached,
    commit_parents,
    complete_merge,
    diff,
    git,
    merge_ff,
    merge_in_progress,
    protected_paths,
    rebase,
    rebase_in_progress,
    require_clean,
    snapshot,
    unmerged_paths,
)
from .state import ExploreState
from .validation import MARKER as VALIDATION_MARKER
from .validation import eligible
from .assets import copy_assets, inventory


TERMINAL = {'success', 'failed', 'killed', 'interrupted'}
VALIDATION_SCRIPT = str(Path(__file__).with_name('validation.py'))
PROMPT_HISTORY_RECORD_LIMIT = 20
PROMPT_HISTORY_SECTION_LIMITS = {
    'findings': 4000,
    'directions': 6000,
    'decisions': 5000,
}
PROMPT_TRIED_DIRECTIONS_LIMIT = 8000


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

    def _phase(self, name):
        return self.config['phases'][name]

    @staticmethod
    def _job_phase(job):
        role = job['role']
        if role == 'planner':
            return 'planning'
        if role == 'optimizer':
            return 'optimization'
        if role == 'fix':
            return 'fix'
        if role == 'validation':
            return 'validation'
        if role == 'resolver':
            return 'merge'
        raise ValueError('unknown exploration role: {}'.format(role))

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
        self._reconcile_fix_counts()
        self._refresh_jobs()
        if self.campaign['status'] == 'active' and self._deadline_reached():
            self.state.update_campaign(self.campaign_id, status='draining')
        self._cleanup_finished_attempts()
        event = self.state.claim_event(self.worker, self.campaign_id)
        if event:
            self._handle_event(event)
            return
        if not self.state.merge_queue_empty(self.campaign_id):
            self._advance_merge()
            return
        campaign = self.campaign
        if campaign['status'] in {'draining', 'waiting_to_land'}:
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
                self._deadline_reached())
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
        if job['role'] in {'planner', 'optimizer'}:
            return True
        return (job['role'] == 'validation' and
                job['metadata'].get('phase') == 'baseline')

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
        timeout = self._phase(self._job_phase(job)).get('timeout', 1800)
        return (now - started).total_seconds() > float(timeout)

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
            elif role == 'optimizer':
                self._finish_mutation(job, output)
            elif role == 'fix':
                self._finish_fix(job, output, event)
            elif role == 'validation':
                self._finish_validation(job, output)
            elif role == 'resolver':
                self._finish_resolver(job, output)
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
                reason = 'controller event failed repeatedly'
                request_id = (job.get('metadata') or {}).get('merge_request_id')
                merge_abandoned = False
                if request_id:
                    request = self.state.get_merge_request(request_id)
                    if request and request['status'] in {'queued', 'processing'}:
                        attempt = self.state.get_attempt(request['attempt_id'])
                        self._abandon_merge(request, attempt, 'failed', reason)
                        merge_abandoned = True
                if job.get('attempt_id'):
                    attempt = self.state.get_attempt(job['attempt_id'])
                    if (not merge_abandoned and attempt and
                            attempt['status'] not in {'merged', 'abandoned'}):
                        self._abandon(attempt, reason)
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
            self._mark_response_failed(job, error)
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
        if self._free_slots() < 1:
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
        optimization = self._phase('optimization')
        prompt = build_optimizer_prompt(
            campaign['objective'], direction['metadata'],
            template=optimization['prompt'],
            memory=self._memory(), artifacts={},
            max_files=optimization['max_files'],
            max_lines=optimization['max_lines'],
        )
        self._queue_agent(
            'optimizer', prompt, worktree, attempt_id, direction_id)
        self.state.update_direction(direction_id, status='running')

    def _finish_mutation(self, job, output):
        event = (
            self.state.get_event(self._current_event_id)
            if self._current_event_id is not None else None)
        attempt, artifacts = self._capture_mutation(job, output, event)
        self._dispatch_candidate(attempt, artifacts)

    def _capture_mutation(self, job, output, event=None, count_fix=False):
        attempt = self.state.get_attempt(job['attempt_id'])
        saved = (event.get('payload') or {}).get('mutation_artifacts') if event else None
        if saved is not None:
            metadata = dict(job.get('metadata') or {})
            metadata['artifacts'] = saved
            self.state.update_job(job['id'], metadata=metadata)
            return attempt, saved
        starting_head = job['metadata'].get('starting_head', attempt['head'])
        action_paths = changed_paths(attempt['worktree'], starting_head)
        optimization = self._phase('optimization')
        head, _ = snapshot(
            attempt['worktree'],
            'tq explore {} {}'.format(self.campaign_id, job['role']),
        )
        # The prior controller may have committed before persisting the event.
        edited = head != starting_head
        stale = is_stalled_output(output, changed=edited) if not count_fix else False
        updates = {'head': head, 'status': 'fixing'}
        if stale:
            updates['stale_count'] = attempt['stale_count'] + 1
        action_diff = diff(attempt['worktree'], starting_head, head)
        action_changed_lines = changed_line_count(
            attempt['worktree'], starting_head, head)
        candidate_diff = diff(attempt['worktree'], attempt['base_head'], head)
        candidate_paths = changed_paths(attempt['worktree'], attempt['base_head'])
        changed_lines = changed_line_count(
            attempt['worktree'], attempt['base_head'], head)
        protected = protected_paths(
            candidate_paths, optimization['protected_paths'])
        max_files = int(optimization['max_files'])
        max_lines = int(optimization['max_lines'])
        limit_violation = (
            (max_files > 0 and len(candidate_paths) > max_files) or
            (max_lines > 0 and (
                changed_lines is None or changed_lines > max_lines)))
        artifacts = dict(job['metadata'].get('artifacts') or {})
        if edited:
            artifacts.pop('validation', None)
            artifacts.pop('validated_head', None)
        artifacts.update({
            'job_status': job['status'],
            'worker_output': output[-20000:],
            'action_changed_paths': action_paths,
            'changed_paths': candidate_paths,
            'protected_paths': protected,
            'diff': candidate_diff,
            'action_diff': action_diff,
            'stalled': stale,
            'changed': head != attempt['base_head'],
            'candidate_changed': head != attempt['base_head'],
            'edited_this_pass': edited,
            'starting_head': starting_head,
            'head': head,
            'stale_count': updates.get('stale_count', attempt['stale_count']),
            'changed_lines': changed_lines,
            'binary_changes': changed_lines is None,
            'action_changed_lines': action_changed_lines,
            'action_binary_changes': action_changed_lines is None,
            'limit_violation': limit_violation,
        })
        fix_count = attempt['fix_count'] + 1 if count_fix and edited else None
        if count_fix and edited:
            artifacts['fix_edit_count'] = fix_count
            artifacts['fix_limit_violation'] = not job['metadata'].get(
                'edits_allowed', True)
        if event:
            attempt = self.state.record_mutation_event(
                event['id'], attempt['id'], head,
                updates.get('stale_count', attempt['stale_count']), artifacts,
                fix_count=fix_count)
        else:
            if fix_count is not None:
                updates['fix_count'] = fix_count
            attempt = self.state.update_attempt(attempt['id'], **updates)
        metadata = dict(job.get('metadata') or {})
        metadata['artifacts'] = artifacts
        self.state.update_job(job['id'], metadata=metadata)
        return attempt, artifacts

    def _dispatch_candidate(self, attempt, artifacts):
        if self._has_validation():
            self._queue_validation(attempt, 'fix', artifacts)
        else:
            artifacts = dict(artifacts, validated_head=None)
            self._queue_fix(attempt, artifacts)

    def _finish_validation(self, job, output):
        attempt = self.state.get_attempt(job['attempt_id']) if job['attempt_id'] else None
        metadata = job['metadata']
        try:
            result = _tail_marker(output, VALIDATION_MARKER)
        except (ValueError, json.JSONDecodeError) as error:
            result = {'checks_passed': False, 'score_error': str(error)}
        if job['status'] != 'success':
            result = {
                'checks_passed': False,
                'score_error': 'validation job ended with status {}'.format(
                    job['status']),
            }
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
        artifacts = dict(metadata.get('artifacts') or {})
        artifacts['validation'] = result
        artifacts['validated_head'] = metadata.get('expected_head')
        if metadata.get('phase') == 'fix':
            self._queue_fix(attempt, artifacts)
            return

    def _queue_validation(self, attempt, phase, artifacts):
        cwd = self._validation_worktree(attempt['head'], attempt['id'])
        baseline_head = self.campaign['mainline_head']
        baseline_cwd = self._validation_worktree(
            baseline_head, '{}-baseline'.format(attempt['id']))
        spec_path = Path(self.config['work_root']) / 'artifacts' / (
            '{}-{}-validation.json'.format(attempt['id'], time.time_ns()))
        spec_path.parent.mkdir(parents=True, exist_ok=True)
        spec_path.write_text(json.dumps({
            'checks': self._phase('validation').get('checks', []),
            'score': self._phase('validation').get('score'),
            'score_repeats': 3,
            'score_seed': time.time_ns(),
            'baseline_cwd': str(baseline_cwd),
        }), encoding='utf-8')
        command = [sys.executable, '-I', VALIDATION_SCRIPT,
                   '--spec', str(spec_path)]
        try:
            self._queue_job(
                'validation', command, cwd, attempt['id'],
                attempt['direction_id'],
                gpus=self._phase('validation').get('gpus', 0), metadata={
                    'phase': phase, 'artifacts': artifacts,
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

    def _queue_fix(self, attempt, artifacts):
        artifacts = dict(artifacts or {})
        artifacts['eligible'] = self._acceptance_allowed(attempt, artifacts)
        max_fixes = self.budgets['max_fixes']
        edits_allowed = not self._limit_reached(
            max_fixes, attempt['fix_count'])
        artifacts['edits_allowed'] = edits_allowed
        direction = self.state.get_direction(attempt['direction_id'])
        phase = self._phase('fix')
        prompt = build_fix_prompt(
            self.campaign['objective'], direction['metadata'],
            template=phase['prompt'], memory=self._memory(),
            artifacts=self._prompt_artifacts(artifacts),
            max_files=self._phase('optimization')['max_files'],
            max_lines=self._phase('optimization')['max_lines'],
        )
        self._queue_agent(
            'fix', prompt, attempt['worktree'], attempt['id'],
            attempt['direction_id'], metadata={
                'phase': 'fix', 'artifacts': artifacts,
                'starting_head': attempt['head'],
                'edits_allowed': edits_allowed,
            })
        self.state.update_attempt(attempt['id'], status='fixing')

    def _acceptance_allowed(self, attempt, artifacts):
        if not self._hard_gates_pass(artifacts):
            return False
        current = git(attempt['worktree'], 'rev-parse', 'HEAD')
        dirty = bool(git(
            attempt['worktree'], 'status', '--porcelain',
            '--untracked-files=normal'))
        if dirty or current != attempt['head']:
            return False
        validation = artifacts.get('validation') or {}
        if self._has_validation() and artifacts.get('validated_head') != current:
            return False
        baseline = self.config.get('baseline_validation') or {}
        return eligible(
            validation or {'checks_passed': True},
            validation.get('baseline_score', baseline.get('score')),
            self._phase('validation').get('score_direction'),
            self._phase('validation').get('min_improvement', 0),
        )

    @staticmethod
    def _hard_gates_pass(artifacts):
        return bool(
            artifacts.get('candidate_changed', artifacts.get('changed')) and
            not artifacts.get('protected_paths') and
            not artifacts.get('limit_violation') and
            not artifacts.get('fix_limit_violation'))

    @staticmethod
    def _compact_acceptance_artifacts(artifacts):
        keys = (
            'validation', 'validated_head', 'head', 'changed_paths',
            'protected_paths', 'changed_lines', 'limit_violation',
            'binary_changes', 'candidate_changed', 'eligible',
            'fix_edit_count',
        )
        return {
            key: artifacts[key] for key in keys
            if key in artifacts and artifacts[key] is not None
        }

    @classmethod
    def _project_validation(cls, validation):
        if not validation:
            return None
        result = {
            key: validation[key] for key in (
                'checks_passed', 'score', 'baseline_score', 'scores',
                'baseline_scores', 'score_error')
            if key in validation
        }
        checks = []
        for item in (validation.get('checks') or [])[:10]:
            checks.append({
                key: value for key, value in {
                    'command': cls._clip_prompt_text(
                        shlex.join(item.get('command') or []), 800),
                    'exit_code': item.get('exit_code'),
                    'timed_out': item.get('timed_out'),
                    'stdout': cls._clip_prompt_text(item.get('stdout'), 800),
                    'stderr': cls._clip_prompt_text(item.get('stderr'), 800),
                }.items() if value not in (None, '')
            })
        if checks:
            result['checks'] = checks
        score_runs = []
        for item in (validation.get('score_runs') or [])[:8]:
            score_runs.append({
                key: value for key, value in {
                    'sample': item.get('sample'),
                    'exit_code': item.get('exit_code'),
                    'timed_out': item.get('timed_out'),
                    'stdout': cls._clip_prompt_text(item.get('stdout'), 400),
                    'stderr': cls._clip_prompt_text(item.get('stderr'), 400),
                }.items() if value not in (None, '')
            })
        if score_runs:
            result['score_runs'] = score_runs
        return result

    @classmethod
    def _prompt_artifacts(cls, artifacts):
        keys = (
            'job_status', 'changed_lines', 'action_changed_lines',
            'binary_changes', 'action_binary_changes', 'limit_violation',
            'fix_limit_violation', 'candidate_changed', 'edited_this_pass',
            'validated_head', 'head', 'eligible', 'edits_allowed',
            'fix_edit_count', 'gate_failure',
        )
        result = {
            key: artifacts[key] for key in keys
            if key in artifacts and artifacts[key] is not None
        }
        for key in (
            'changed_paths', 'action_changed_paths', 'protected_paths',
        ):
            values = artifacts.get(key) or []
            if values:
                projected = [
                    cls._clip_prompt_text(value, 300) for value in values[:100]
                ]
                if len(values) > len(projected):
                    projected.append(
                        '[{} more omitted]'.format(len(values) - len(projected)))
                result[key] = projected
        result['diff'] = cls._clip_prompt_text(artifacts.get('diff'), 24000)
        action_diff = artifacts.get('action_diff')
        if action_diff and action_diff != artifacts.get('diff'):
            result['latest_fix_diff'] = cls._clip_prompt_text(action_diff, 8000)
        worker_output = artifacts.get('worker_output')
        if worker_output:
            result['worker_output'] = cls._clip_prompt_text(
                worker_output[-4000:], 4000)
        validation = cls._project_validation(artifacts.get('validation'))
        if validation:
            result['validation'] = validation
        return result

    def _finish_fix(self, job, output, event):
        metadata = job['metadata']
        attempt = self.state.get_attempt(job['attempt_id'])
        if metadata.get('response_only'):
            if not self._restore_control_worktree(job):
                output = ''
            artifacts = dict(metadata.get('artifacts') or {})
        else:
            attempt, artifacts = self._capture_mutation(
                job, output, event, count_fix=True)
            if artifacts.get('edited_this_pass'):
                if artifacts.get('fix_limit_violation'):
                    self._abandon(attempt, 'fix edit limit reached')
                elif self._has_validation():
                    self._queue_validation(attempt, 'fix', artifacts)
                else:
                    self._queue_fix(attempt, artifacts)
                return
        if job['status'] != 'success':
            self._abandon(
                attempt, 'fix job ended with status {}'.format(job['status']))
            return
        try:
            decision = parse_fix_response(output)
        except AgentResponseError as error:
            self._mark_response_failed(job, error)
            if self._queue_response_repair(job, error):
                return
            self._abandon(
                attempt, 'invalid fix response: {}'.format(error))
            return
        stored_decision = self.state.add_decision(
            self.campaign_id, decision['decision'], attempt_id=attempt['id'],
            event_id=event['id'], phase='fix',
            generation=self.campaign['generation'], reason=decision['reason'],
            evidence=decision['evidence'],
            memory_updates=decision['memory_updates'],
            next_direction=decision['next_direction'])
        self._record_memory(
            attempt, decision, 'fix', decision_id=stored_decision['id'])
        self._apply_fix_decision(attempt, decision, artifacts)

    def _apply_fix_decision(self, attempt, decision, artifacts):
        attempt = self.state.get_attempt(attempt['id'])
        if attempt['status'] not in {'active', 'fixing'}:
            return
        value = decision['decision']
        if value == 'accept':
            if not self._acceptance_allowed(attempt, artifacts):
                if attempt['stale_count'] >= 1:
                    self._abandon(
                        attempt, 'candidate failed a system-controlled acceptance gate')
                else:
                    attempt = self.state.update_attempt(
                        attempt['id'], stale_count=attempt['stale_count'] + 1)
                    retry_artifacts = dict(artifacts)
                    retry_artifacts['gate_failure'] = (
                        'candidate failed a system-controlled acceptance gate')
                    self._queue_fix(attempt, retry_artifacts)
                return
            merged = self.state.list_merge_requests(
                self.campaign_id, status='merged')
            if self._limit_reached(
                self.budgets['max_accepted_attempts'], len(merged)
            ):
                self._abandon(attempt, 'merge limit reached')
                self.state.update_campaign(self.campaign_id, status='draining')
                return
            self.state.enqueue_merge_request(
                self.campaign_id, attempt['id'], attempt['head'], metadata={
                    'acceptance_artifacts': self._compact_acceptance_artifacts(
                        artifacts),
                    'reason': decision['reason'],
                })
        elif value == 'stop':
            self.state.update_campaign(self.campaign_id, status='draining')
            self.state.update_attempt(attempt['id'], status='stopped')
        else:
            self._abandon(attempt, decision['reason'])

    def _reconcile_fix_counts(self):
        numbers = {}
        for job in self.state.list_jobs(
            campaign_id=self.campaign_id, role='fix'):
            number = job['metadata'].get(
                'artifacts', {}).get('fix_edit_count')
            if job['attempt_id'] and number:
                numbers[job['attempt_id']] = max(
                    numbers.get(job['attempt_id'], 0), int(number))
        for attempt_id, number in numbers.items():
            attempt = self.state.get_attempt(attempt_id)
            if attempt and attempt['fix_count'] < number:
                self.state.update_attempt(attempt_id, fix_count=number)

    def _advance_merge(self):
        merged = self.state.list_merge_requests(self.campaign_id, status='merged')
        if self._limit_reached(
            self.budgets['max_accepted_attempts'], len(merged)
        ):
            for request in self.state.list_merge_requests(
                self.campaign_id, status='queued'):
                attempt = self.state.get_attempt(request['attempt_id'])
                self._abandon_merge(
                    request, attempt, 'cancelled', 'merge limit reached')
            self.state.update_campaign(self.campaign_id, status='draining')
            return
        processing = self.state.list_merge_requests(
            self.campaign_id, status='processing')
        if processing:
            request = processing[0]
            waiting = any(
                job['inspected_at'] is None and
                job['metadata'].get('merge_request_id') == request['id']
                for job in self.state.list_jobs(campaign_id=self.campaign_id)
            )
            if not waiting:
                self._resume_merge(request)
            return
        request = self.state.claim_merge_request(self.campaign_id, self.worker)
        if not request:
            return
        self._resume_merge(request)

    def _resume_merge(self, request):
        attempt = self.state.get_attempt(request['attempt_id'])
        metadata = request.get('metadata') or {}
        stage = metadata.get('stage')
        if stage is None:
            self._begin_merge(request, attempt)
        elif stage == 'snapshotting':
            self._snapshot_merge_source(request, attempt)
        elif stage == 'fast_forwarding':
            self._try_fast_forward(request, attempt)
        elif stage == 'rebasing':
            self._recover_rebase(request, attempt)
        elif stage == 'merge_fallback':
            self._recover_merge_fallback(request, attempt)
        elif stage == 'resolving':
            self._recover_merge_resolution(request, attempt)
        elif stage == 'landing':
            self._land_merge(request, attempt)
        else:
            reason = 'unknown merge stage: {}'.format(stage)
            self._abandon_merge(request, attempt, 'failed', reason)

    def _begin_merge(self, request, attempt):
        target = self.campaign['mainline_head']
        branch_head = git(attempt['worktree'], 'rev-parse', 'HEAD')
        if not self._mainline_unchanged(target):
            self._fail_merge_queue('mainline moved or became dirty before integration')
            return
        if branch_head != request['head']:
            reason = 'candidate changed after acceptance'
            self._abandon_merge(request, attempt, 'failed', reason)
            return
        metadata = dict(
            request['metadata'], stage='snapshotting', target_head=target,
            accepted_head=request['metadata'].get(
                'accepted_head', request['head']))
        request = self.state.update_merge_request(
            request['id'], metadata=metadata)
        self._snapshot_merge_source(request, attempt)

    def _snapshot_merge_source(self, request, attempt):
        metadata = request['metadata']
        target = metadata['target_head']
        accepted = metadata['accepted_head']
        if not self._mainline_unchanged(target):
            self._fail_merge_queue('mainline moved while snapshotting candidate')
            return
        cwd = attempt['worktree']
        head = git(cwd, 'rev-parse', 'HEAD')
        message = self._snapshot_message(request['id'])
        if head == accepted:
            try:
                head, _changed = snapshot(cwd, message)
            except BackendError as error:
                self._reject_merge(
                    request['id'], attempt, str(error),
                    'could not snapshot candidate')
                return
        elif not self._is_snapshot_recovery(cwd, accepted, head, message):
            self._abandon_merge(
                request, attempt, 'failed',
                'candidate head moved unexpectedly while snapshotting')
            return
        if git(cwd, 'status', '--porcelain', '--untracked-files=normal'):
            self._abandon_merge(
                request, attempt, 'failed',
                'candidate snapshot did not leave a clean worktree')
            return
        metadata = dict(
            metadata, stage='fast_forwarding', source_head=head,
            snapshot_changed=head != accepted)
        request = self.state.record_merge_head(
            request['id'], attempt['id'], accepted, head, metadata)
        self._try_fast_forward(
            request, self.state.get_attempt(attempt['id']))

    def _try_fast_forward(self, request, attempt):
        metadata = request['metadata']
        target = metadata['target_head']
        source = metadata['source_head']
        if not self._mainline_unchanged(target):
            self._fail_merge_queue('mainline moved before fast-forward')
            return
        if not self._exact_clean_head(attempt['worktree'], source):
            self._abandon_merge(
                request, attempt, 'failed',
                'snapshotted candidate changed before integration')
            return
        if self._is_ancestor(attempt['worktree'], target, source):
            self._record_integration(request, attempt, source, 'fast-forward')
            return
        request = self.state.update_merge_request(
            request['id'], metadata=dict(metadata, stage='rebasing'))
        self._recover_rebase(request, attempt)

    def _recover_rebase(self, request, attempt):
        metadata = request.get('metadata') or {}
        target = metadata['target_head']
        source = metadata['source_head']
        if not self._mainline_unchanged(target):
            self._fail_merge_queue('mainline moved during rebase')
            return
        if rebase_in_progress(attempt['worktree']):
            if not unmerged_paths(attempt['worktree']):
                self._reject_merge(
                    request['id'], attempt, '',
                    'rebase stopped without merge conflicts')
                return
            request = self.state.update_merge_request(
                request['id'], metadata=dict(metadata, stage='merge_fallback'))
            self._recover_merge_fallback(request, attempt)
            return
        dirty = bool(git(
            attempt['worktree'], 'status', '--porcelain',
            '--untracked-files=normal'))
        if dirty:
            self._reject_merge(
                request['id'], attempt, '',
                'unexpected dirty worktree while recovering rebase')
            return
        head = git(attempt['worktree'], 'rev-parse', 'HEAD')
        based_on_target = self._is_ancestor(
            attempt['worktree'], target, head)
        if head == source and based_on_target:
            self._record_integration(request, attempt, head, 'fast-forward')
        elif head == source:
            self._run_rebase(request, attempt)
        elif based_on_target:
            self._record_integration(request, attempt, head, 'rebase')
        else:
            self._reject_merge(
                request['id'], attempt, '',
                'unexpected candidate head while recovering rebase')

    def _run_rebase(self, request, attempt):
        target = request['metadata']['target_head']
        ok, output = rebase(attempt['worktree'], target)
        if not ok:
            if rebase_in_progress(attempt['worktree']):
                if not unmerged_paths(attempt['worktree']):
                    self._reject_merge(
                        request['id'], attempt, output,
                        'rebase stopped without merge conflicts')
                    return
                request = self.state.update_merge_request(
                    request['id'], metadata=dict(
                        request['metadata'], stage='merge_fallback',
                        rebase_output=output[-4000:]))
                self._recover_merge_fallback(request, attempt)
            else:
                self._reject_merge(
                    request['id'], attempt, output, 'git rebase failed')
            return
        head = git(attempt['worktree'], 'rev-parse', 'HEAD')
        clean = not git(
            attempt['worktree'], 'status', '--porcelain',
            '--untracked-files=normal')
        if (not clean or rebase_in_progress(attempt['worktree']) or
                not self._is_ancestor(attempt['worktree'], target, head)):
            self._reject_merge(
                request['id'], attempt, output,
                'git rebase reported success with an invalid worktree')
            return
        self._record_integration(request, attempt, head, 'rebase')

    def _recover_merge_fallback(self, request, attempt):
        metadata = request['metadata']
        target = metadata['target_head']
        source = metadata['source_head']
        cwd = attempt['worktree']
        if not self._mainline_unchanged(target):
            self._fail_merge_queue('mainline moved during merge fallback')
            return
        if rebase_in_progress(cwd):
            ok, output = abort_rebase(cwd)
            if not ok:
                self._reject_merge(
                    request['id'], attempt, output, 'could not abort rebase')
                return
        if merge_in_progress(cwd):
            if unmerged_paths(cwd):
                self._queue_merge_resolver(request, attempt)
            else:
                self._complete_pending_merge(request, attempt)
            return
        head = git(cwd, 'rev-parse', 'HEAD')
        clean = not git(
            cwd, 'status', '--porcelain', '--untracked-files=normal')
        if clean and self._valid_merge_commit(cwd, head, target, source):
            self._record_integration(request, attempt, head, 'merge')
            return
        if not clean or head not in {source, target}:
            self._reject_merge(
                request['id'], attempt, '',
                'merge fallback did not recover the original candidate')
            return
        if head == source:
            try:
                checkout_detached(cwd, target)
            except BackendError as error:
                self._reject_merge(
                    request['id'], attempt, str(error),
                    'could not prepare detached merge worktree')
                return
        if not self._exact_clean_head(cwd, target):
            self._reject_merge(
                request['id'], attempt, '',
                'could not prepare detached merge worktree')
            return
        ok, output = begin_no_ff_merge(cwd, source)
        if ok:
            self._complete_pending_merge(request, attempt)
        elif merge_in_progress(cwd):
            if unmerged_paths(cwd):
                request = self.state.update_merge_request(
                    request['id'], metadata=dict(
                        metadata, stage='resolving', merge_output=output[-4000:]))
                self._queue_merge_resolver(request, attempt, output)
            else:
                self._complete_pending_merge(request, attempt)
        else:
            self._reject_merge(
                request['id'], attempt, output, 'git merge failed')

    def _recover_merge_resolution(self, request, attempt):
        metadata = request['metadata']
        target = metadata['target_head']
        source = metadata['source_head']
        cwd = attempt['worktree']
        if not self._mainline_unchanged(target):
            self._fail_merge_queue('mainline moved during conflict resolution')
            return
        if merge_in_progress(cwd):
            if unmerged_paths(cwd):
                self._queue_merge_resolver(request, attempt)
            else:
                self._complete_pending_merge(request, attempt)
            return
        head = git(cwd, 'rev-parse', 'HEAD')
        if (self._exact_clean_head(cwd, head) and
                self._valid_merge_commit(cwd, head, target, source)):
            self._record_integration(request, attempt, head, 'merge')
            return
        self._reject_merge(
            request['id'], attempt, '',
            'merge resolver did not leave a valid aggregate merge')

    def _queue_merge_resolver(self, request, attempt, output=''):
        cwd = attempt['worktree']
        if not merge_in_progress(cwd) or not unmerged_paths(cwd):
            self._reject_merge(
                request['id'], attempt, output,
                'merge failed without resolvable conflicts')
            return
        if not output:
            output = git(cwd, 'status', '--short', '--untracked-files=normal')
        if request['metadata'].get('stage') != 'resolving':
            request = self.state.update_merge_request(
                request['id'], metadata=dict(
                    request['metadata'], stage='resolving'))
        prompt = build_merge_prompt(
            self.campaign['objective'],
            self.state.get_direction(attempt['direction_id'])['metadata'],
            template=self._phase('merge')['prompt'],
            memory=self._memory(), artifacts={
                'rebase': request['metadata'].get('rebase_output'),
                'merge': output,
            },
            max_files=self._phase('optimization')['max_files'],
            max_lines=self._phase('optimization')['max_lines'],
        )
        self._queue_agent(
            'resolver', prompt, cwd, attempt['id'], attempt['direction_id'],
            metadata={'merge_request_id': request['id']})

    def _finish_resolver(self, job, output):
        request_id = job['metadata']['merge_request_id']
        request = self.state.get_merge_request(request_id)
        if request is None or request['status'] != 'processing':
            return
        if request['metadata'].get('stage') != 'resolving':
            self._resume_merge(request)
            return
        attempt = self.state.get_attempt(job['attempt_id'])
        if job['status'] != 'success':
            self._reject_merge(
                request_id, attempt, output, 'merge resolver job failed')
            return
        target = request['metadata']['target_head']
        if not self._mainline_unchanged(target):
            self._fail_merge_queue('mainline moved during conflict resolution')
            return
        cwd = attempt['worktree']
        if merge_in_progress(cwd):
            if unmerged_paths(cwd):
                self._reject_merge(
                    request_id, attempt, output,
                    'merge resolver left unresolved conflicts')
                return
            self._complete_pending_merge(request, attempt)
            return
        head = git(cwd, 'rev-parse', 'HEAD')
        source = request['metadata']['source_head']
        if (self._exact_clean_head(cwd, head) and
                self._valid_merge_commit(cwd, head, target, source)):
            self._record_integration(request, attempt, head, 'merge')
            return
        self._reject_merge(
            request_id, attempt, output,
            'merge resolver did not produce a valid merge commit')

    def _complete_pending_merge(self, request, attempt):
        cwd = attempt['worktree']
        if unmerged_paths(cwd):
            self._reject_merge(
                request['id'], attempt, '', 'merge still has unresolved paths')
            return
        try:
            head = complete_merge(
                cwd, 'tq explore {} merge {}'.format(
                    self.campaign_id, request['id']))
        except BackendError as error:
            self._reject_merge(
                request['id'], attempt, str(error), 'could not commit merge')
            return
        target = request['metadata']['target_head']
        source = request['metadata']['source_head']
        if (not self._exact_clean_head(cwd, head) or
                not self._valid_merge_commit(cwd, head, target, source)):
            self._reject_merge(
                request['id'], attempt, '',
                'aggregate merge commit failed integrity checks')
            return
        self._record_integration(request, attempt, head, 'merge')

    def _record_integration(self, request, attempt, head, method):
        metadata = dict(
            request['metadata'], stage='landing', integration_head=head,
            integration_method=method)
        request = self.state.record_merge_head(
            request['id'], attempt['id'], request['metadata']['source_head'],
            head, metadata)
        self._land_merge(request, self.state.get_attempt(attempt['id']))

    def _land_merge(self, request, attempt):
        metadata = request['metadata']
        target = metadata['target_head']
        head = metadata['integration_head']
        mainline = self.config['mainline_worktree']
        current = git(mainline, 'rev-parse', 'HEAD')
        if git(mainline, 'status', '--porcelain', '--untracked-files=normal'):
            self._fail_merge_queue(
                'mainline became dirty before integration landing')
            return
        if current == head:
            self._record_merged(request, attempt, head, metadata)
            return
        if current != target or target != self.campaign['mainline_head']:
            self._fail_merge_queue('mainline moved before integration landing')
            return
        if (request['head'] != head or
                not self._exact_clean_head(attempt['worktree'], head) or
                not self._is_ancestor(attempt['worktree'], target, head)):
            self._abandon_merge(
                request, attempt, 'failed',
                'integrated candidate changed before landing')
            return
        artifacts = {'head': head}
        accepted = metadata.get('acceptance_artifacts') or {}
        if accepted.get('validated_head') == head:
            artifacts.update(accepted)
        metadata = dict(
            metadata, expected_head=head,
            merge_artifacts=self._compact_acceptance_artifacts(artifacts))
        request = self.state.update_merge_request(
            request['id'], metadata=metadata)
        try:
            landed = merge_ff(mainline, head)
        except BackendError as error:
            self._fail_merge_queue(
                'fast-forward failed: {}'.format(str(error)))
            return
        if landed != head:
            raise BackendError(
                'fast-forward landed {} instead of {}'.format(landed, head))
        self._record_merged(request, attempt, landed, metadata)

    def _reject_merge(self, request_id, attempt, output, prefix):
        request = self.state.get_merge_request(request_id)
        if request is None or request['status'] != 'processing':
            return
        detail = (output or '').strip()[-4000:]
        reason = prefix
        if detail:
            reason += ': {}'.format(detail)
        self._abandon_merge(request, attempt, 'rejected', reason)

    @staticmethod
    def _snapshot_message(request_id):
        return 'tq explore merge snapshot {}'.format(request_id)

    def _is_snapshot_recovery(self, cwd, accepted, head, message):
        return bool(
            not git(cwd, 'status', '--porcelain', '--untracked-files=normal') and
            commit_parents(cwd, head) == [accepted] and
            git(cwd, 'show', '-s', '--format=%s', head) == message)

    def _mainline_unchanged(self, target):
        return bool(
            target == self.campaign['mainline_head'] and
            target == git(
                self.config['mainline_worktree'], 'rev-parse', 'HEAD') and
            not git(
                self.config['mainline_worktree'], 'status', '--porcelain',
                '--untracked-files=normal'))

    @staticmethod
    def _exact_clean_head(cwd, head):
        return bool(
            git(cwd, 'rev-parse', 'HEAD') == head and
            not git(cwd, 'status', '--porcelain', '--untracked-files=normal') and
            not rebase_in_progress(cwd) and not merge_in_progress(cwd))

    @staticmethod
    def _valid_merge_commit(cwd, head, target, source):
        return head not in {target, source} and commit_parents(cwd, head) == [
            target, source]

    def _record_merged(self, request, attempt, head, metadata):
        artifacts = metadata.get('merge_artifacts') or metadata.get(
            'acceptance_artifacts', {})
        config = self._promote_baseline({'artifacts': artifacts})
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
            self._free_slots(), max(0, int(self.budgets['parallel']) - active))
        if free <= 0:
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
            self._free_slots(), max(0, int(self.budgets['parallel']) - active))
        if free <= 0 or self._active_job('planner'):
            return
        if self._has_validation() and 'baseline_validation' not in self.config:
            self._queue_baseline()
            return
        prompt = build_planner_prompt(
            self.campaign['objective'],
            template=self._phase('planning')['prompt'],
            memory=self._memory(include_directions=False),
            tried_directions=self._direction_history(),
            direction_count=free,
            max_files=self._phase('optimization')['max_files'],
            max_lines=self._phase('optimization')['max_lines'],
        )
        self._queue_agent(
            'planner', prompt, self.config['control_cwd'], control=True,
            metadata={'direction_count': free})

    def _queue_baseline(self):
        spec_path = Path(self.config['work_root']) / 'artifacts' / 'baseline.json'
        spec_path.parent.mkdir(parents=True, exist_ok=True)
        spec_path.write_text(json.dumps({
            'checks': self._phase('validation').get('checks', []),
            'score': self._phase('validation').get('score'),
            'score_repeats': 3,
        }), encoding='utf-8')
        head = self.campaign['mainline_head']
        cwd = self._validation_worktree(head, 'baseline')
        try:
            self._queue_job(
                'validation', [sys.executable, '-I', VALIDATION_SCRIPT,
                               '--spec', str(spec_path)], cwd,
                gpus=self._phase('validation').get('gpus', 0),
                metadata={
                    'phase': 'baseline', 'artifacts': {},
                    'validation_cwd': str(cwd), 'expected_head': head,
                },
            )
        except Exception:
            self._remove_validation_worktree(cwd)
            raise

    def _advance_landing(self):
        if self._active_mutation_jobs() or not self.state.merge_queue_empty(self.campaign_id):
            return
        self._land_target()

    def _land_target(self):
        root = self.config['repo_root']
        target = self.campaign['target_ref']
        try:
            with target_integration_lock(root, target):
                self._land_target_locked(root, target)
        except BackendError as error:
            self._manual_landing(str(error))

    def _land_target_locked(self, root, target):
        require_clean(root)
        branch = git(root, 'symbolic-ref', '--short', 'HEAD')
        if branch != target:
            raise BackendError(
                'target branch {} is not checked out'.format(target))
        target_head = git(root, 'rev-parse', 'HEAD')
        mainline = self.config['mainline_branch']
        if self._is_ancestor(root, mainline, target_head):
            self._complete_landing(target_head)
            return
        if not self._is_ancestor(root, target_head, mainline):
            raise BackendError('target and campaign mainline have diverged')
        self._complete_landing(
            fast_forward_checked_out(root, target_head, mainline))

    def _manual_landing(self, reason):
        root = self.config['repo_root']
        target = self.campaign['target_ref']
        mainline = self.config['mainline_branch']
        command = '{} && {}'.format(
            shlex.join(['git', '-C', root, 'switch', target]),
            shlex.join(['git', '-C', root, 'merge', mainline]),
        )
        manual = {
            'reason': reason,
            'command': command,
            'message': 'Automatic fast-forward is not possible; merge manually.',
        }
        previous = self.config.get('manual_landing')
        config = dict(self.config)
        config['manual_landing'] = manual
        self.state.update_campaign(
            self.campaign_id, status='waiting_to_land', config=config)
        if previous != manual:
            self.state.emit(
                self.campaign_id, 'campaign.manual_landing', manual,
                outbox_topic='campaign.manual_landing',
                dedupe_key='manual-landing:{}'.format(self.campaign_id))

    def _complete_landing(self, head):
        config = dict(self.config)
        config.pop('manual_landing', None)
        self.state.update_campaign(
            self.campaign_id, target_head=head, mainline_head=head,
            status='completed', finished_at=self._now(), config=config)
        self._cleanup_mainline()

    def _queue_agent(
        self, role, prompt, cwd, attempt_id=None, direction_id=None,
        control=False, metadata=None,
    ):
        data = dict(metadata or {})
        phase = self._job_phase({'role': role, 'metadata': data})
        argv = render_command(self._phase(phase)['command'], prompt)
        data['agent'] = True
        if role in {'planner', 'fix'}:
            data['response_prompt'] = prompt
        if role in {'optimizer', 'fix'} and attempt_id and not data.get(
            'response_only'
        ):
            data.setdefault(
                'starting_head', self.state.get_attempt(attempt_id)['head'])
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
        gpus=0, slots=1, internal=False, metadata=None,
    ):
        metadata = dict(metadata or {})
        if self._current_event_id is not None:
            metadata['source_event_id'] = self._current_event_id
        job_environment = dict(getattr(self.backend, 'env', {}) or {})
        job_environment.update(os.environ)
        job_environment.update(self.config.get('env') or {})
        backend_id = self.backend.add(
            shlex.join([str(value) for value in argv]),
            gpus=int(gpus), slots=slots,
            env=job_environment,
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
            {'optimizer', 'fix', 'validation', 'resolver'}
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

    def _queue_response_repair(self, job, error):
        metadata = dict(job['metadata'])
        if metadata.get('repair_count', 0) >= 1:
            return False
        prompt = metadata.get('response_prompt')
        if not prompt:
            return False
        metadata['repair_count'] = 1
        if job['role'] == 'fix':
            metadata['response_only'] = True
        phase = self._phase(self._job_phase(job))
        prompt = render_prompt(
            phase['response_repair_prompt'],
            original_prompt=prompt, error=str(error))
        self._queue_agent(
            job['role'], prompt, self.config['control_cwd'],
            job.get('attempt_id'), job.get('direction_id'), control=True,
            metadata=metadata)
        return True

    def _mark_response_failed(self, job, error):
        if job['status'] != 'success':
            return
        reason = 'invalid {} response: {}'.format(job['role'], error)
        self.backend.mark_workflow_failed(
            {'id': int(job['backend_job_id'])},
            reason=reason,
            phase='response_validation',
        )
        metadata = dict(job.get('metadata') or {})
        metadata['response_error'] = str(error)
        self.state.update_job(
            job['id'], status='failed', metadata=metadata)

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
                clean = clean and self._validation_assets_clean(cwd)
            return clean
        cwd = metadata.get('validation_cwd')
        expected = metadata.get('expected_head')
        if not cwd or not expected:
            return True
        current = git(cwd, 'rev-parse', 'HEAD')
        dirty = bool(git(cwd, 'status', '--porcelain', '--untracked-files=normal'))
        return (current == expected and not dirty and
                self._validation_assets_clean(cwd))

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
        manifest = self.config.get('asset_manifest') or []
        if manifest:
            copy_assets(
                self.config['asset_snapshot'], path / '.tq' / 'explore-assets',
                expected=manifest)
        return path

    def _validation_assets_clean(self, cwd):
        expected = self.config.get('asset_manifest') or []
        if not expected:
            return True
        try:
            return inventory(Path(cwd) / '.tq' / 'explore-assets') == expected
        except BackendError:
            return False

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
        merged = self.state.list_merge_requests(self.campaign_id, status='merged')
        if self._limit_reached(
            budgets['max_accepted_attempts'], len(merged)
        ):
            return True
        return self._deadline_reached()

    @staticmethod
    def _limit_reached(limit, value):
        limit = int(limit)
        return limit > 0 and value >= limit

    def _deadline_reached(self):
        deadline = self.budgets.get('deadline')
        return deadline is not None and time.time() >= float(deadline)

    def _has_validation(self):
        validation = self._phase('validation')
        return bool(validation.get('checks') or validation.get('score'))

    @staticmethod
    def _clip_prompt_text(value, limit=1200):
        if value is None:
            return None
        text = str(value)
        return text if len(text) <= limit else text[:limit] + '[truncated]'

    @classmethod
    def _bounded_history(cls, records, section, newest_first=False):
        values = list(records)
        if not newest_first:
            values.reverse()
        total_records = len(values)
        values = values[:PROMPT_HISTORY_RECORD_LIMIT]
        budget = PROMPT_HISTORY_SECTION_LIMITS.get(
            section, PROMPT_TRIED_DIRECTIONS_LIMIT)
        kept = []
        used = 2
        for record in values:
            size = len(json.dumps(
                record, sort_keys=True, ensure_ascii=False, default=str)) + 1
            if used + size > budget:
                break
            kept.append(record)
            used += size
        omitted = total_records - len(kept)
        if omitted:
            marker = {'omitted_records': omitted}
            marker_size = len(json.dumps(marker)) + 1
            while kept and used + marker_size > budget:
                removed = kept.pop()
                used -= len(json.dumps(
                    removed, sort_keys=True, ensure_ascii=False,
                    default=str)) + 1
                marker['omitted_records'] += 1
            kept.append(marker)
        return kept

    @classmethod
    def _project_finding(cls, item):
        return {
            key: value for key, value in {
                'claim': cls._clip_prompt_text(item.get('claim')),
                'trust': item.get('trust'),
                'confidence': item.get('confidence'),
                'outcome': item.get('outcome'),
                'scope': cls._clip_prompt_text(item.get('scope'), 400),
                'source_commit': item.get('source_commit'),
            }.items() if value is not None
        }

    @classmethod
    def _project_direction(cls, item):
        metadata = item.get('metadata') or {}
        return {
            key: value for key, value in {
                'id': item.get('id'),
                'hypothesis': cls._clip_prompt_text(
                    item.get('hypothesis') or metadata.get('hypothesis')),
                'approach': cls._clip_prompt_text(
                    metadata.get('approach') or metadata.get('direction')),
                'different_from': metadata.get('different_from'),
                'generation': item.get('generation'),
                'status': item.get('status'),
            }.items() if value is not None
        }

    @classmethod
    def _project_decision(cls, item):
        evidence = [
            cls._clip_prompt_text(value, 500)
            for value in (item.get('evidence') or [])[:5]
        ]
        return {
            key: value for key, value in {
                'decision': item.get('decision'),
                'phase': item.get('phase'),
                'reason': cls._clip_prompt_text(item.get('reason')),
                'evidence': evidence or None,
                'next_direction': item.get('next_direction'),
            }.items() if value is not None
        }

    def _direction_history(self):
        records = [
            self._project_direction(item)
            for item in self.state.list_directions(self.campaign_id)
        ]
        return self._bounded_history(records, 'tried_directions')

    def _memory(self, include_directions=True):
        findings = [
            self._project_finding(item)
            for item in self.state.list_findings(campaign_id=self.campaign_id)
        ]
        decisions = [
            self._project_decision(item)
            for item in self.state.list_decisions(self.campaign_id)
        ]
        memory = {
            'findings': self._bounded_history(
                findings, 'findings', newest_first=True),
            'decisions': self._bounded_history(decisions, 'decisions'),
        }
        if include_directions:
            directions = [
                self._project_direction(item)
                for item in self.state.list_directions(self.campaign_id)
            ]
            memory['directions'] = self._bounded_history(
                directions, 'directions')
        return memory

    def _record_memory(self, attempt, decision, phase, decision_id=None):
        for index, value in enumerate(decision.get('memory_updates') or []):
            claim = value.get('claim') if isinstance(value, dict) else str(value)
            if not claim:
                continue
            self.state.add_finding(
                claim, 'interpreted', campaign_id=self.campaign_id,
                attempt_id=attempt['id'] if attempt else None,
                direction_id=attempt['direction_id'] if attempt else None,
                outcome=decision['decision'],
                provenance={'phase': phase, 'agent': True},
                dedupe_key='decision:{}:{}:{}'.format(
                    self.campaign_id, decision_id or 'unknown', index),
            )

    def _promote_baseline(self, metadata):
        config = dict(self.config)
        validation = metadata.get('artifacts', {}).get('validation')
        if validation:
            config['baseline_validation'] = validation
        elif self._has_validation():
            config.pop('baseline_validation', None)
        return config

    def _stall_campaign(self, reason):
        campaign = self.campaign
        count = campaign['stall_count'] + 1
        status = 'failed' if count >= 4 else campaign['status']
        self.state.update_campaign(
            self.campaign_id, stall_count=count, status=status)
        self.state.emit(self.campaign_id, 'campaign.stalled', {'reason': reason})

    def _fail_merge_queue(self, reason):
        self.state.update_campaign(self.campaign_id, status='failed')
        for request in self.state.list_merge_requests(
            self.campaign_id, active=True
        ):
            attempt = self.state.get_attempt(request['attempt_id'])
            self._abandon_merge(request, attempt, 'failed', reason)

    def _abandon_merge(self, request, attempt, status, reason):
        self.state.abandon_merge_request(
            request['id'], status, {'reason': reason})
        self._record_abandonment(attempt, reason)

    def _abandon(self, attempt, reason):
        self.state.update_attempt(attempt['id'], status='abandoned')
        self.state.update_direction(attempt['direction_id'], status='abandoned')
        self._record_abandonment(attempt, reason)

    def _record_abandonment(self, attempt, reason):
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
        for status in ('merged', 'abandoned', 'stopped'):
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
    interval = float(campaign['config']['phases']['controller']['interval'])
    try:
        while controller.campaign['status'] not in {
            'completed', 'failed',
        }:
            controller.reconcile()
            time.sleep(interval)
    finally:
        if state.get_campaign(campaign_id)['status'] in {
            'completed', 'failed',
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

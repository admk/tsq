import datetime
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

import taskq.explore.controller as explore_controller
from taskq.backends import git_ref
from taskq.backends.base import BackendError
from taskq.explore.controller import (
    PROMPT_HISTORY_SECTION_LIMITS,
    PROMPT_TRIED_DIRECTIONS_LIMIT,
    ExploreController,
    TERMINAL,
)
from taskq.explore.git import git, snapshot
from taskq.explore.state import ExploreState
from taskq.explore.validation import MARKER as VALIDATION_MARKER


class FakeBackend:
    def __init__(self, slots=4):
        self.config = {'slots': slots}
        self.jobs = {}
        self.outputs = {}
        self.add_calls = []
        self.unregistered = []
        self._next_id = 1

    def add(self, command, gpus, slots, **kwargs):
        job_id = self._next_id
        self._next_id += 1
        self.jobs[job_id] = {
            'id': job_id,
            'status': 'queued',
            'exitcode': None,
            'slots_required': slots,
            'metadata': kwargs.get('metadata', {}),
        }
        self.add_calls.append({
            'id': job_id,
            'command': command,
            'gpus': gpus,
            'slots': slots,
            **kwargs,
        })
        return job_id

    def finish(self, job_id, output='', status='success'):
        self.jobs[int(job_id)].update({
            'status': status,
            'exitcode': 0 if status == 'success' else 1,
        })
        self.outputs[int(job_id)] = output

    def job_info(self, ids):
        return [dict(self.jobs[job_id]) for job_id in ids if job_id in self.jobs]

    def full_info(self, _ids):
        return [dict(job) for job in self.jobs.values()]

    def output(self, info, _tail):
        return self.outputs.get(int(info['id']), '')

    def mark_workflow_failed(self, info, reason=None, phase='workflow'):
        job = self.jobs[int(info['id'])]
        assert job['status'] in {'success', 'failed'}
        if job['status'] == 'success':
            job.update({
                'status': 'failed',
                'command_exitcode': job['exitcode'],
                'exitcode': None,
                'failure_phase': phase,
                'failure_reason': reason,
            })
        return dict(job)

    def unregister_controller(self, campaign_id):
        self.unregistered.append(campaign_id)


@dataclass
class Harness:
    repo: Path
    work_root: Path
    mainline: Path
    state: ExploreState
    backend: FakeBackend
    controller: ExploreController

    def attempt(self, suffix):
        direction_id = 'c1-d{}'.format(suffix)
        attempt_id = '{}-a'.format(direction_id)
        branch = 'tq/explore/c1/attempt/d{}'.format(suffix)
        worktree = self.work_root / 'attempts' / direction_id
        campaign = self.state.get_campaign('c1')
        workspace = git_ref.create_branch_worktree(
            self.repo, branch, worktree, campaign['mainline_head'])
        self.state.add_direction(
            'c1', direction_id, 'direction {}'.format(suffix),
            'fingerprint-{}'.format(suffix),
            metadata={
                'hypothesis': 'direction {}'.format(suffix),
                'approach': 'approach {}'.format(suffix),
            },
        )
        return self.state.add_attempt(
            'c1', attempt_id, direction_id, branch, worktree,
            campaign['mainline_head'], metadata={'workspace': workspace},
        )

    def commit_change(self, attempt, text, path='app.txt', message='candidate'):
        candidate = Path(attempt['worktree'], path)
        candidate.parent.mkdir(parents=True, exist_ok=True)
        candidate.write_text(text, encoding='utf-8')
        head, changed = snapshot(attempt['worktree'], message)
        assert changed
        return self.state.update_attempt(attempt['id'], head=head)

    def queue_optimizer(self, attempt, output='implemented candidate'):
        job_id = self.controller._queue_agent(
            'optimizer', 'optimize', attempt['worktree'],
            attempt['id'], attempt['direction_id'])
        job = self.state.get_job(job_id)
        self.backend.finish(job['backend_job_id'], output)
        return job_id

    def finish_fix(self, job, decision='accept', reason=None, output=None):
        if output is None:
            output = fix_output(decision, reason=reason)
        self.backend.finish(job['backend_job_id'], output)

    def finish_validation(self, job, checks_passed=True, **result):
        payload = {'checks_passed': checks_passed, **result}
        self.backend.finish(
            job['backend_job_id'], VALIDATION_MARKER + json.dumps(payload))

    def move_mainline(self, text, path='mainline.txt'):
        target = self.mainline / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding='utf-8')
        head, changed = snapshot(self.mainline, 'mainline moved')
        assert changed
        self.state.update_campaign('c1', mainline_head=head)
        return head


def fix_output(decision='accept', reason=None):
    return 'TASKQ_JSON: ' + json.dumps({
        'decision': decision,
        'reason': reason or '{} evidence'.format(decision),
        'evidence': [],
        'memory_updates': [],
        'next_direction': None,
    })


def _run(cwd, *args):
    return subprocess.run(
        ['git', '-C', str(cwd), *args], check=True,
        capture_output=True, text=True,
    ).stdout.strip()


def _parents(cwd, head='HEAD'):
    return _run(cwd, 'rev-list', '--parents', '-n', '1', head).split()[1:]


def _configure_validation(campaign):
    config = dict(campaign.controller.config)
    config['phases'] = dict(config['phases'])
    config['phases']['validation'] = dict(
        config['phases']['validation'], checks=['true'])
    config['baseline_validation'] = {'checks_passed': True}
    campaign.state.update_campaign('c1', config=config)


def _configure_optimization(campaign, **changes):
    config = dict(campaign.controller.config)
    config['phases'] = dict(config['phases'])
    config['phases']['optimization'] = dict(
        config['phases']['optimization'], **changes)
    campaign.state.update_campaign('c1', config=config)


def _stage_merge_resolution(campaign, attempt, text='resolved\n'):
    Path(attempt['worktree'], 'app.txt').write_text(text, encoding='utf-8')
    _run(attempt['worktree'], 'add', 'app.txt')


@pytest.fixture
def campaign(tmp_path):
    repo = tmp_path / 'repo'
    repo.mkdir()
    _run(repo, 'init', '-b', 'main')
    _run(repo, 'config', 'user.name', 'Taskq Tests')
    _run(repo, 'config', 'user.email', 'taskq@example.invalid')
    _run(repo, 'config', 'commit.gpgsign', 'false')
    (repo / 'app.txt').write_text('base\n', encoding='utf-8')
    _run(repo, 'add', 'app.txt')
    _run(repo, 'commit', '-m', 'base')
    head = _run(repo, 'rev-parse', 'HEAD')

    work_root = tmp_path / 'work'
    mainline = work_root / 'mainline'
    control_cwd = work_root / 'control'
    control_cwd.mkdir(parents=True)
    mainline_branch = 'tq/explore/c1/mainline'
    workspace = git_ref.create_branch_worktree(
        repo, mainline_branch, mainline, head)
    state = ExploreState(tmp_path / 'state.sqlite')
    backend = FakeBackend()
    state.create_campaign(
        'c1', 'make it faster', 'main', mainline_branch,
        target_head=head,
        budgets={
            'parallel': 2,
            'max_fixes': 2,
            'max_accepted_attempts': 5,
            'deadline': time.time() + 3600,
        },
        config={
            'repo_root': str(repo),
            'work_root': str(work_root),
            'mainline_branch': mainline_branch,
            'mainline_worktree': str(mainline),
            'control_cwd': str(control_cwd),
            'heartbeat_file': str(work_root / 'heartbeat'),
            'phases': {
                'optimization': {
                    'command': ['fake-agent', '{}'], 'timeout': 1800,
                    'prompt': '$objective $direction $memory $artifacts',
                    'protected_paths': [], 'max_files': 5, 'max_lines': 300,
                },
                'planning': {
                    'command': ['fake-agent', '{}'], 'timeout': 1800,
                    'prompt': '$objective $direction_count $memory $tried_directions',
                    'response_repair_prompt': '$original_prompt $error',
                },
                'fix': {
                    'command': ['fake-agent', '{}'], 'timeout': 1800,
                    'prompt': '$objective $direction $memory $artifacts',
                    'response_repair_prompt': '$original_prompt $error',
                },
                'validation': {
                    'gpus': 0, 'checks': [], 'score': None,
                    'score_direction': None, 'min_improvement': 0,
                    'timeout': 1800,
                },
                'merge': {
                    'command': ['fake-agent', '{}'], 'timeout': 1800,
                    'prompt': '$objective $direction $memory $artifacts',
                },
                'controller': {
                    'interval': 5, 'heartbeat_timeout': 30,
                    'max_wall_time': 3600,
                },
            },
            'workspace': workspace,
        },
    )
    harness = Harness(
        repo, work_root, mainline, state, backend,
        ExploreController(state, backend, 'c1'),
    )
    try:
        yield harness
    finally:
        state.close()


@pytest.mark.parametrize('terminal_status', sorted(TERMINAL))
def test_optimizer_terminal_status_dispatches_one_fix(campaign, terminal_status):
    attempt = campaign.attempt(terminal_status)
    Path(attempt['worktree'], 'app.txt').write_text(
        '{}\n'.format(terminal_status), encoding='utf-8')
    job_id = campaign.controller._queue_agent(
        'optimizer', 'optimize', attempt['worktree'],
        attempt['id'], attempt['direction_id'])
    backend_id = campaign.state.get_job(job_id)['backend_job_id']
    campaign.backend.finish(
        backend_id, 'completed a bounded change', terminal_status)

    campaign.controller.reconcile()

    events = campaign.state.list_events('c1')
    fixes = campaign.state.list_jobs(campaign_id='c1', role='fix')
    assert len(events) == 1
    assert events[0]['status'] == 'completed'
    assert len(fixes) == 1
    assert fixes[0]['metadata']['artifacts']['job_status'] == terminal_status
    assert campaign.state.get_attempt(attempt['id'])['status'] == 'fixing'


@pytest.mark.parametrize(('role', 'phase'), [
    ('planner', 'planning'),
    ('optimizer', 'optimization'),
    ('fix', 'fix'),
    ('resolver', 'merge'),
])
def test_agent_roles_use_their_phase_command(campaign, role, phase):
    config = dict(campaign.controller.config)
    config['phases'] = dict(config['phases'])
    config['phases'][phase] = dict(config['phases'][phase])
    config['phases'][phase]['command'] = [phase + '-agent', '{}']
    campaign.state.update_campaign('c1', config=config)
    attempt = campaign.attempt('role-{}'.format(role)) if role in {
        'optimizer', 'fix', 'resolver'} else None

    campaign.controller._queue_agent(
        role, 'prompt', attempt['worktree'] if attempt else campaign.mainline,
        attempt['id'] if attempt else None,
        attempt['direction_id'] if attempt else None,
        metadata={'merge_request_id': 1} if role == 'resolver' else None)

    assert campaign.backend.add_calls[-1]['command'] == phase + '-agent prompt'


def test_job_timeout_comes_from_its_phase(campaign):
    config = dict(campaign.controller.config)
    config['phases'] = dict(config['phases'])
    for phase, timeout in (('planning', 1), ('fix', 60)):
        config['phases'][phase] = dict(config['phases'][phase], timeout=timeout)
    campaign.state.update_campaign('c1', config=config)
    started = datetime.datetime.now() - datetime.timedelta(seconds=2)

    assert campaign.controller._overdue(
        {'start_time': started}, {'role': 'planner', 'metadata': {}})
    assert not campaign.controller._overdue(
        {'start_time': started}, {'role': 'fix', 'metadata': {}})


def test_only_validation_jobs_request_configured_gpus(campaign):
    config = dict(campaign.controller.config)
    config['phases'] = dict(config['phases'])
    config['phases']['validation'] = dict(
        config['phases']['validation'], gpus=2)
    campaign.state.update_campaign('c1', config=config)

    campaign.controller._queue_baseline()
    validation = campaign.backend.add_calls[-1]
    campaign.controller._queue_agent('optimizer', 'change code', campaign.mainline)
    optimization = campaign.backend.add_calls[-1]

    assert validation['metadata']['role'] == 'validation'
    assert validation['gpus'] == 2
    assert optimization['metadata']['role'] == 'optimizer'
    assert optimization['gpus'] == 0


def test_campaign_environment_overrides_are_sent_to_phase_jobs(
    campaign, monkeypatch,
):
    monkeypatch.setenv('PATH', '/controller/bin')
    config = dict(campaign.controller.config)
    config['env'] = {
        'PATH': '/main/.venv/bin:/usr/bin',
        'VIRTUAL_ENV': '/main/.venv',
    }
    campaign.state.update_campaign('c1', config=config)

    campaign.controller._queue_agent('planner', 'prompt', campaign.mainline)

    environment = campaign.backend.add_calls[-1]['env']
    assert environment['PATH'] == '/main/.venv/bin:/usr/bin'
    assert environment['VIRTUAL_ENV'] == '/main/.venv'


def test_optimizer_flows_directly_to_fix_without_validation(campaign):
    attempt = campaign.attempt('no-validation')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)

    campaign.controller.reconcile()

    assert campaign.state.list_jobs(campaign_id='c1', role='validation') == []
    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    assert fix['attempt_id'] == attempt['id']
    assert fix['metadata']['artifacts']['candidate_changed'] is True
    assert fix['metadata']['artifacts']['validated_head'] is None


def test_optimizer_flows_through_validation_to_fix(campaign):
    _configure_validation(campaign)
    attempt = campaign.attempt('validated')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)

    campaign.controller.reconcile()

    validation = campaign.state.list_jobs(
        campaign_id='c1', role='validation')[0]
    assert validation['metadata']['phase'] == 'fix'
    assert campaign.state.list_jobs(campaign_id='c1', role='fix') == []
    campaign.finish_validation(validation)
    campaign.controller.reconcile()

    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    assert fix['metadata']['artifacts']['validation']['checks_passed'] is True
    assert fix['metadata']['artifacts']['validated_head'] == (
        campaign.state.get_attempt(attempt['id'])['head'])


def test_fix_runs_in_attempt_worktree_and_consumes_one_slot(campaign):
    attempt = campaign.attempt('fix-worktree')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()

    queued = campaign.backend.add_calls[-1]
    assert queued['metadata']['role'] == 'fix'
    assert queued['cwd'] == attempt['worktree']
    assert queued['slots'] == 1
    assert queued['internal'] is False


def test_fix_edit_and_accept_output_forces_fresh_validation_and_fix(campaign):
    _configure_validation(campaign)
    attempt = campaign.attempt('edit-accept')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    first_validation = campaign.state.list_jobs(
        campaign_id='c1', role='validation')[0]
    campaign.finish_validation(first_validation)
    campaign.controller.reconcile()
    first_fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]

    Path(attempt['worktree'], 'app.txt').write_text('repaired\n', encoding='utf-8')
    campaign.finish_fix(first_fix, decision='accept')
    campaign.controller.reconcile()

    assert campaign.state.list_decisions('c1') == []
    validations = campaign.state.list_jobs(campaign_id='c1', role='validation')
    assert len(validations) == 2
    assert validations[-1]['metadata']['expected_head'] != (
        first_validation['metadata']['expected_head'])
    assert campaign.state.get_attempt(attempt['id'])['fix_count'] == 1
    campaign.finish_validation(validations[-1])
    campaign.controller.reconcile()
    assert len(campaign.state.list_jobs(campaign_id='c1', role='fix')) == 2


def test_fix_detects_agent_self_commit_as_an_edit(campaign):
    attempt = campaign.attempt('self-commit')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    first_fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]

    Path(attempt['worktree'], 'app.txt').write_text('self committed\n', encoding='utf-8')
    _run(attempt['worktree'], 'add', 'app.txt')
    _run(attempt['worktree'], 'commit', '-m', 'agent commit')
    assert not _run(attempt['worktree'], 'status', '--porcelain')
    campaign.finish_fix(first_fix, decision='accept')
    campaign.controller.reconcile()

    fixes = campaign.state.list_jobs(campaign_id='c1', role='fix')
    assert len(fixes) == 2
    assert fixes[-1]['metadata']['starting_head'] == _run(
        attempt['worktree'], 'rev-parse', 'HEAD')
    assert campaign.state.list_merge_requests('c1') == []


def test_invalid_fix_json_is_salvaged_when_the_fix_edited(campaign):
    attempt = campaign.attempt('invalid-after-edit')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    first_fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]

    Path(attempt['worktree'], 'app.txt').write_text('repaired\n', encoding='utf-8')
    campaign.finish_fix(first_fix, output='not json')
    campaign.controller.reconcile()

    fixes = campaign.state.list_jobs(campaign_id='c1', role='fix')
    assert len(fixes) == 2
    assert fixes[0]['status'] == 'success'
    assert fixes[-1]['metadata'].get('response_only') is not True
    assert campaign.state.get_attempt(attempt['id'])['fix_count'] == 1


def test_no_edit_accept_at_exact_head_queues_once(campaign):
    attempt = campaign.attempt('accept')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    accepted_head = campaign.state.get_attempt(attempt['id'])['head']

    campaign.finish_fix(fix, decision='accept')
    campaign.controller.reconcile()
    campaign.controller.reconcile()

    requests = campaign.state.list_merge_requests('c1')
    assert len(requests) == 1
    assert requests[0]['head'] == accepted_head
    assert requests[0]['metadata']['acceptance_artifacts']['head'] == accepted_head
    decisions = campaign.state.list_decisions('c1')
    assert [(item['phase'], item['decision']) for item in decisions] == [
        ('fix', 'accept')]


@pytest.mark.parametrize(('gate', 'configure', 'first_path', 'second_path'), [
    (
        'protected_paths',
        {'protected_paths': ['app.txt']},
        'app.txt',
        'safe.txt',
    ),
    (
        'limit_violation',
        {'max_files': 1},
        'app.txt',
        'second.txt',
    ),
])
def test_fix_gates_use_cumulative_candidate_state(
    campaign, gate, configure, first_path, second_path,
):
    _configure_optimization(campaign, **configure)
    attempt = campaign.attempt('cumulative-{}'.format(gate))
    first = Path(attempt['worktree'], first_path)
    first.write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    first_fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]

    Path(attempt['worktree'], second_path).write_text('repair\n', encoding='utf-8')
    campaign.finish_fix(first_fix, decision='fixed')
    campaign.controller.reconcile()

    next_fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[-1]
    artifacts = next_fix['metadata']['artifacts']
    assert artifacts[gate]
    assert set(artifacts['changed_paths']) == {first_path, second_path}
    assert artifacts['eligible'] is False


def test_line_cap_uses_exact_stats_when_prompt_diff_is_truncated(campaign):
    _configure_optimization(campaign, max_lines=3)
    attempt = campaign.attempt('exact-line-cap')
    Path(attempt['worktree'], 'app.txt').write_text(
        ''.join('{}\n'.format('x' * 20000) for _ in range(4)),
        encoding='utf-8')
    campaign.queue_optimizer(attempt)

    campaign.controller.reconcile()

    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    artifacts = fix['metadata']['artifacts']
    assert artifacts['diff'].endswith('[diff truncated]\n')
    assert artifacts['changed_lines'] == 5
    assert artifacts['limit_violation'] is True
    assert artifacts['eligible'] is False


def test_failed_validation_cannot_be_accepted(campaign):
    _configure_validation(campaign)
    attempt = campaign.attempt('failed-validation')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    validation = campaign.state.list_jobs(
        campaign_id='c1', role='validation')[0]
    campaign.finish_validation(validation, checks_passed=False)
    campaign.controller.reconcile()
    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]

    campaign.finish_fix(fix, decision='accept')
    campaign.controller.reconcile()

    assert campaign.state.list_merge_requests('c1') == []
    retry = campaign.state.list_jobs(campaign_id='c1', role='fix')[-1]
    assert retry['id'] != fix['id']
    assert retry['metadata']['artifacts']['gate_failure']


def test_failed_validation_job_cannot_pass_with_a_success_marker(campaign):
    _configure_validation(campaign)
    attempt = campaign.attempt('failed-validation-job')
    Path(attempt['worktree'], 'app.txt').write_text(
        'candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    validation = campaign.state.list_jobs(
        campaign_id='c1', role='validation')[0]
    campaign.backend.finish(
        validation['backend_job_id'],
        VALIDATION_MARKER + json.dumps({'checks_passed': True}),
        status='failed')

    campaign.controller.reconcile()

    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    result = fix['metadata']['artifacts']['validation']
    assert result['checks_passed'] is False
    assert 'status failed' in result['score_error']


def test_max_fixes_blocks_the_next_edit(campaign):
    budgets = dict(campaign.controller.budgets, max_fixes=1)
    campaign.state.update_campaign('c1', budgets=budgets)
    attempt = campaign.attempt('fix-cap')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    first = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    Path(attempt['worktree'], 'app.txt').write_text('repair one\n', encoding='utf-8')
    campaign.finish_fix(first, decision='fixed')
    campaign.controller.reconcile()
    second = campaign.state.list_jobs(campaign_id='c1', role='fix')[-1]
    assert second['metadata']['edits_allowed'] is False

    Path(attempt['worktree'], 'app.txt').write_text('repair two\n', encoding='utf-8')
    campaign.finish_fix(second, decision='fixed')
    campaign.controller.reconcile()

    assert campaign.state.get_attempt(attempt['id'])['status'] == 'abandoned'
    assert campaign.state.get_attempt(attempt['id'])['fix_count'] == 2


def test_zero_max_fixes_allows_unlimited_edits(campaign):
    budgets = dict(campaign.controller.budgets, max_fixes=0)
    campaign.state.update_campaign('c1', budgets=budgets)
    attempt = campaign.attempt('unlimited-fixes')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()

    for number in range(3):
        fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[-1]
        assert fix['metadata']['edits_allowed'] is True
        Path(attempt['worktree'], 'app.txt').write_text(
            'repair {}\n'.format(number), encoding='utf-8')
        campaign.finish_fix(fix, decision='fixed')
        campaign.controller.reconcile()

    current = campaign.state.get_attempt(attempt['id'])
    assert current['fix_count'] == 3
    assert current['status'] == 'fixing'


def test_event_replay_creates_only_one_child_job(campaign, monkeypatch):
    attempt = campaign.attempt('event-replay')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    dispatch = campaign.controller._dispatch_candidate
    calls = 0

    def dispatch_then_crash(current_attempt, artifacts):
        nonlocal calls
        calls += 1
        dispatch(current_attempt, artifacts)
        if calls == 1:
            raise RuntimeError('controller crashed after dispatch')

    monkeypatch.setattr(
        campaign.controller, '_dispatch_candidate', dispatch_then_crash)

    campaign.controller.reconcile()
    event = campaign.state.list_events('c1')[0]
    assert event['status'] == 'pending'
    assert len(campaign.state.list_jobs(campaign_id='c1', role='fix')) == 1

    campaign.controller.reconcile()

    assert campaign.state.get_event(event['id'])['status'] == 'completed'
    assert len(campaign.state.list_jobs(campaign_id='c1', role='fix')) == 1
    assert calls == 1


def test_invalid_no_edit_fix_gets_one_response_repair_turn(campaign):
    attempt = campaign.attempt('repair-response')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    first = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    campaign.finish_fix(first, output='not json')

    campaign.controller.reconcile()

    fixes = campaign.state.list_jobs(campaign_id='c1', role='fix')
    assert len(fixes) == 2
    assert fixes[0]['status'] == 'failed'
    assert fixes[-1]['metadata']['response_only'] is True
    assert fixes[-1]['metadata']['repair_count'] == 1
    assert 'does not contain' in fixes[-1]['metadata']['response_prompt']


def test_failed_no_edit_fix_cannot_accept(campaign):
    attempt = campaign.attempt('failed-accept')
    Path(attempt['worktree'], 'app.txt').write_text(
        'candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    campaign.backend.finish(
        fix['backend_job_id'], fix_output('accept'), status='failed')

    campaign.controller.reconcile()

    assert campaign.state.list_merge_requests('c1') == []
    assert campaign.state.get_attempt(attempt['id'])['status'] == 'abandoned'


def test_stop_decision_cleans_up_the_attempt_worktree(campaign):
    attempt = campaign.attempt('stop-cleanup')
    Path(attempt['worktree'], 'app.txt').write_text(
        'candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt)
    campaign.controller.reconcile()
    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    campaign.finish_fix(fix, decision='stop')

    campaign.controller.reconcile()

    assert campaign.state.get_attempt(attempt['id'])['status'] == 'stopped'
    assert campaign.state.get_campaign('c1')['status'] == 'draining'
    assert not Path(attempt['worktree']).exists()


def test_planner_repair_gets_error_and_accepts_backend_trailer(campaign):
    first_id = campaign.controller._queue_agent(
        'planner', 'plan', campaign.mainline, control=True,
        metadata={'direction_count': 1})
    first = campaign.state.get_job(first_id)
    campaign.backend.finish(first['backend_job_id'], 'not json')
    campaign.controller.reconcile()

    planners = campaign.state.list_jobs(campaign_id='c1', role='planner')
    assert len(planners) == 2
    assert planners[0]['status'] == 'failed'
    repair = planners[-1]
    assert repair['metadata']['repair_count'] == 1
    assert 'does not contain' in repair['metadata']['response_prompt']

    campaign.backend.finish(
        repair['backend_job_id'],
        'TASKQ_JSON: {"directions":[{"hypothesis":"batch reads"}]}\n'
        'tokens used: 123\n[taskq] job finished with exit code 0')
    campaign.controller.reconcile()

    assert [item['hypothesis'] for item in
            campaign.state.list_directions('c1')] == ['batch reads']
    assert campaign.state.get_campaign('c1')['stall_count'] == 0


def test_controller_adopts_backend_job_lost_before_sqlite_insert(campaign):
    campaign.backend.jobs[99] = {
        'id': 99, 'status': 'queued', 'exitcode': None,
        'slots_required': 0,
        'metadata': {
            'campaign_id': 'c1', 'role': 'planner',
            'attempt_id': None, 'direction_id': None,
            'workflow_metadata': {'direction_count': 1},
        },
    }

    campaign.controller.reconcile()

    adopted = campaign.state.get_job('c1:99')
    assert adopted['backend_job_id'] == '99'
    assert adopted['role'] == 'planner'
    assert adopted['metadata']['direction_count'] == 1


def test_mutation_snapshot_survives_controller_retry(campaign, monkeypatch):
    attempt = campaign.attempt('snapshot-retry')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    campaign.queue_optimizer(attempt, output='original worker evidence')
    dispatch = campaign.controller._dispatch_candidate
    calls = 0

    def fail_once(current_attempt, artifacts):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError('dispatch interrupted')
        dispatch(current_attempt, artifacts)

    monkeypatch.setattr(campaign.controller, '_dispatch_candidate', fail_once)
    campaign.controller.reconcile()
    event = campaign.state.list_events('c1')[0]
    saved = event['payload']['mutation_artifacts']
    assert saved['worker_output'] == 'original worker evidence'

    campaign.controller.reconcile()

    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    assert campaign.state.get_event(event['id'])['status'] == 'completed'
    artifacts = fix['metadata']['artifacts']
    assert artifacts['worker_output'] == saved['worker_output']
    assert artifacts['diff'] == saved['diff']
    assert artifacts['head'] == saved['head']


def test_merge_barrier_prevents_planner_allocation(campaign):
    attempt = campaign.attempt('barrier')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], attempt['head'])
    assert campaign.state.claim_merge_request('c1', 'busy')['id'] == request['id']

    campaign.controller.reconcile()

    assert campaign.state.list_jobs(campaign_id='c1', role='planner') == []
    assert campaign.backend.add_calls == []


def test_unexpected_mainline_drift_fails_and_drains_merge_queue(campaign):
    first = campaign.commit_change(
        campaign.attempt('drift-first'), 'first\n')
    second = campaign.commit_change(
        campaign.attempt('drift-second'), 'second\n')
    requests = [
        campaign.state.enqueue_merge_request(
            'c1', attempt['id'], attempt['head'])
        for attempt in (first, second)
    ]
    Path(campaign.mainline, 'external.txt').write_text(
        'unexpected\n', encoding='utf-8')
    snapshot(campaign.mainline, 'unexpected external mainline change')

    campaign.controller.reconcile()

    assert campaign.state.get_campaign('c1')['status'] == 'failed'
    assert [
        campaign.state.get_merge_request(request['id'])['status']
        for request in requests
    ] == ['failed', 'failed']
    assert [
        campaign.state.get_attempt(attempt['id'])['status']
        for attempt in (first, second)
    ] == ['abandoned', 'abandoned']
    assert campaign.state.merge_queue_empty('c1')


def test_dirty_mainline_fails_and_drains_merge_queue(campaign):
    attempt = campaign.commit_change(campaign.attempt('dirty-mainline'), 'candidate\n')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], attempt['head'])
    target = _run(campaign.mainline, 'rev-parse', 'HEAD')
    Path(campaign.mainline, 'untracked.txt').write_text(
        'external dirt\n', encoding='utf-8')

    campaign.controller.reconcile()

    assert _run(campaign.mainline, 'rev-parse', 'HEAD') == target
    assert campaign.state.get_campaign('c1')['status'] == 'failed'
    failed = campaign.state.get_merge_request(request['id'])
    assert failed['status'] == 'failed'
    assert failed['result']['reason'] == (
        'mainline moved or became dirty before integration')


def test_planner_finishing_behind_merge_barrier_does_not_start_attempt(campaign):
    planner_id = campaign.controller._queue_agent(
        'planner', 'plan', campaign.mainline, control=True,
        metadata={'direction_count': 1})
    planner = campaign.state.get_job(planner_id)
    queued = campaign.attempt('queued-after-plan')
    campaign.state.enqueue_merge_request('c1', queued['id'], queued['head'])
    campaign.backend.finish(
        planner['backend_job_id'],
        'TASKQ_JSON: {"directions":[{"hypothesis":"batch reads"}]}')

    campaign.controller.reconcile()

    direction = campaign.state.list_directions('c1')[-1]
    assert direction['hypothesis'] == 'batch reads'
    assert direction['status'] == 'deferred'
    assert campaign.state.list_attempts(direction_id=direction['id']) == []
    assert campaign.state.list_jobs(campaign_id='c1', role='optimizer') == []


def test_merge_snapshots_tracked_and_untracked_changes_before_fast_forward(campaign):
    attempt = campaign.commit_change(campaign.attempt('snapshot'), 'accepted\n')
    accepted = attempt['head']
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], accepted)
    Path(attempt['worktree'], 'app.txt').write_text('snapshotted\n', encoding='utf-8')
    Path(attempt['worktree'], '.gitignore').write_text(
        'ignored.tmp\n', encoding='utf-8')
    Path(attempt['worktree'], 'new.txt').write_text('new\n', encoding='utf-8')
    Path(attempt['worktree'], 'ignored.tmp').write_text('ignored\n', encoding='utf-8')

    campaign.controller.reconcile()

    merged = campaign.state.get_merge_request(request['id'])
    source = merged['metadata']['source_head']
    assert merged['status'] == 'merged'
    assert source != accepted
    assert _parents(campaign.repo, source) == [accepted]
    assert (campaign.mainline / 'app.txt').read_text(encoding='utf-8') == (
        'snapshotted\n')
    assert (campaign.mainline / 'new.txt').read_text(encoding='utf-8') == 'new\n'
    assert _run(campaign.repo, 'ls-tree', '-r', '--name-only', source).splitlines() == [
        '.gitignore', 'app.txt', 'new.txt']


def test_fast_forward_is_tried_first_and_skips_rebase(campaign, monkeypatch):
    attempt = campaign.commit_change(campaign.attempt('fast-forward'), 'faster\n')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], attempt['head'])
    monkeypatch.setattr(
        explore_controller, 'rebase',
        lambda *_args: pytest.fail('rebase should not run'))
    monkeypatch.setattr(
        explore_controller, 'begin_no_ff_merge',
        lambda *_args: pytest.fail('merge fallback should not run'))

    campaign.controller.reconcile()

    merged = campaign.state.get_merge_request(request['id'])
    assert merged['status'] == 'merged'
    assert merged['metadata']['integration_method'] == 'fast-forward'
    assert merged['head'] == attempt['head']
    assert campaign.state.list_jobs(campaign_id='c1') == []


def test_fast_forward_operational_failure_does_not_trigger_rebase(
    campaign, monkeypatch,
):
    attempt = campaign.commit_change(campaign.attempt('ff-failure'), 'faster\n')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], attempt['head'])

    def fail_fast_forward(*_args):
        raise BackendError('local collision')

    monkeypatch.setattr(explore_controller, 'merge_ff', fail_fast_forward)
    monkeypatch.setattr(
        explore_controller, 'rebase',
        lambda *_args: pytest.fail('operational FF failure is not divergence'))

    campaign.controller.reconcile()

    assert campaign.state.get_campaign('c1')['status'] == 'failed'
    failed = campaign.state.get_merge_request(request['id'])
    assert failed['status'] == 'failed'
    assert failed['result']['reason'] == 'fast-forward failed: local collision'


def test_failed_fast_forward_clean_rebase_then_fast_forward(campaign):
    _configure_validation(campaign)
    attempt = campaign.commit_change(campaign.attempt('clean-rebase'), 'faster\n')
    source = attempt['head']
    target = campaign.move_mainline('mainline\n')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], source, metadata={
            'acceptance_artifacts': {
                'head': source, 'validation': {'checks_passed': True},
                'validated_head': source,
            },
        })

    campaign.controller.reconcile()

    merged = campaign.state.get_merge_request(request['id'])
    assert merged['status'] == 'merged'
    assert merged['metadata']['integration_method'] == 'rebase'
    assert merged['head'] != source
    assert _run(campaign.repo, 'merge-base', '--is-ancestor', target, merged['head']) == ''
    assert campaign.state.list_jobs(campaign_id='c1') == []
    assert 'baseline_validation' not in campaign.state.get_campaign('c1')['config']


def test_rebase_does_not_update_sibling_refs(campaign):
    attempt = campaign.commit_change(
        campaign.attempt('no-update-refs'), 'first\n', message='first')
    first = attempt['head']
    _run(campaign.repo, 'branch', 'sibling-ref', first)
    Path(attempt['worktree'], 'second.txt').write_text('second\n', encoding='utf-8')
    source, changed = snapshot(attempt['worktree'], 'second')
    assert changed
    attempt = campaign.state.update_attempt(attempt['id'], head=source)
    campaign.move_mainline('mainline\n')
    _run(campaign.repo, 'config', 'rebase.updateRefs', 'true')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], source)

    campaign.controller.reconcile()

    assert campaign.state.get_merge_request(request['id'])['status'] == 'merged'
    assert _run(campaign.repo, 'rev-parse', 'sibling-ref') == first


def test_rebase_conflict_falls_back_to_clean_aggregate_merge(campaign):
    attempt = campaign.commit_change(
        campaign.attempt('aggregate-clean'), 'candidate\n', message='first')
    Path(attempt['worktree'], 'app.txt').write_text('base\n', encoding='utf-8')
    Path(attempt['worktree'], 'feature.txt').write_text('feature\n', encoding='utf-8')
    source, changed = snapshot(attempt['worktree'], 'second')
    assert changed
    attempt = campaign.state.update_attempt(attempt['id'], head=source)
    target = campaign.move_mainline('mainline\n', path='app.txt')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], source)

    campaign.controller.reconcile()

    merged = campaign.state.get_merge_request(request['id'])
    assert merged['status'] == 'merged'
    assert merged['metadata']['integration_method'] == 'merge'
    assert _parents(campaign.repo, merged['head']) == [target, source]
    assert (campaign.mainline / 'app.txt').read_text(encoding='utf-8') == 'mainline\n'
    assert (campaign.mainline / 'feature.txt').read_text(encoding='utf-8') == (
        'feature\n')
    assert campaign.state.list_jobs(campaign_id='c1', role='resolver') == []


def test_rebase_conflict_falls_back_to_one_merge_resolver(campaign):
    attempt = campaign.commit_change(
        campaign.attempt('aggregate-conflict'), 'candidate\n')
    source = attempt['head']
    target = campaign.move_mainline('mainline\n', path='app.txt')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], source)

    campaign.controller.reconcile()

    resolver = campaign.state.list_jobs(campaign_id='c1', role='resolver')[0]
    assert not explore_controller.rebase_in_progress(attempt['worktree'])
    assert explore_controller.merge_in_progress(attempt['worktree'])
    assert _run(attempt['worktree'], 'rev-parse', 'MERGE_HEAD') == source
    _stage_merge_resolution(campaign, attempt)
    campaign.backend.finish(resolver['backend_job_id'], 'resolved aggregate conflict')
    campaign.controller.reconcile()

    merged = campaign.state.get_merge_request(request['id'])
    assert merged['status'] == 'merged'
    assert _parents(campaign.repo, merged['head']) == [target, source]
    assert (campaign.mainline / 'app.txt').read_text(encoding='utf-8') == 'resolved\n'
    assert [job['role'] for job in campaign.state.list_jobs(campaign_id='c1')] == [
        'resolver']


@pytest.mark.parametrize('invalid_result', ['dirty', 'unfinished'])
def test_merge_resolver_rejects_invalid_result(campaign, invalid_result):
    attempt = campaign.commit_change(
        campaign.attempt('invalid-{}'.format(invalid_result)), 'candidate\n')
    campaign.move_mainline('mainline\n', path='app.txt')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], attempt['head'])
    campaign.controller.reconcile()
    resolver = campaign.state.list_jobs(campaign_id='c1', role='resolver')[0]

    if invalid_result != 'unfinished':
        _stage_merge_resolution(campaign, attempt)
    if invalid_result == 'dirty':
        Path(attempt['worktree'], 'dirty.txt').write_text('dirty\n', encoding='utf-8')
    campaign.backend.finish(resolver['backend_job_id'], 'resolver finished')
    campaign.controller.reconcile()

    assert campaign.state.get_merge_request(request['id'])['status'] == 'rejected'
    assert campaign.state.get_attempt(attempt['id'])['status'] == 'abandoned'
    assert _run(campaign.mainline, 'rev-parse', 'HEAD') == (
        campaign.state.get_campaign('c1')['mainline_head'])


def test_failed_rebase_without_conflict_is_rejected(campaign, monkeypatch):
    attempt = campaign.commit_change(campaign.attempt('failed-rebase'), 'faster\n')
    campaign.move_mainline('mainline\n')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], attempt['head'])
    monkeypatch.setattr(
        explore_controller, 'rebase',
        lambda _cwd, _target: (False, 'synthetic rebase failure'))

    campaign.controller.reconcile()

    rejected = campaign.state.get_merge_request(request['id'])
    assert rejected['status'] == 'rejected'
    assert rejected['result']['reason'] == (
        'git rebase failed: synthetic rebase failure')
    assert campaign.state.list_jobs(campaign_id='c1', role='resolver') == []


def test_reconcile_recovers_snapshot_before_state_persistence(campaign, monkeypatch):
    attempt = campaign.commit_change(campaign.attempt('snapshot-crash'), 'accepted\n')
    accepted = attempt['head']
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], accepted)
    Path(attempt['worktree'], 'new.txt').write_text('new\n', encoding='utf-8')
    record = campaign.state.record_merge_head
    calls = 0

    def crash_once(*args, **kwargs):
        nonlocal calls
        metadata = args[4]
        if metadata['stage'] == 'fast_forwarding' and calls == 0:
            calls += 1
            raise RuntimeError('crashed before snapshot persistence')
        return record(*args, **kwargs)

    monkeypatch.setattr(campaign.state, 'record_merge_head', crash_once)
    with pytest.raises(RuntimeError, match='snapshot persistence'):
        campaign.controller.reconcile()

    snapshotted = _run(attempt['worktree'], 'rev-parse', 'HEAD')
    assert _parents(campaign.repo, snapshotted) == [accepted]
    assert campaign.state.get_merge_request(request['id'])['metadata']['stage'] == (
        'snapshotting')

    campaign.controller.reconcile()

    merged = campaign.state.get_merge_request(request['id'])
    assert merged['status'] == 'merged'
    assert merged['metadata']['source_head'] == snapshotted
    assert _run(campaign.repo, 'rev-list', '--count', '{}..{}'.format(
        accepted, merged['head'])) == '1'


def test_reconcile_recovers_clean_rebase_before_state_persistence(
    campaign, monkeypatch,
):
    attempt = campaign.commit_change(campaign.attempt('rebase-crash'), 'faster\n')
    source = attempt['head']
    campaign.move_mainline('mainline\n')
    request = campaign.state.enqueue_merge_request('c1', attempt['id'], source)
    record = campaign.state.record_merge_head
    calls = 0

    def crash_once(*args, **kwargs):
        nonlocal calls
        metadata = args[4]
        if metadata['stage'] == 'landing' and calls == 0:
            calls += 1
            raise RuntimeError('crashed before integration persistence')
        return record(*args, **kwargs)

    monkeypatch.setattr(campaign.state, 'record_merge_head', crash_once)
    with pytest.raises(RuntimeError, match='integration persistence'):
        campaign.controller.reconcile()

    rebased = _run(attempt['worktree'], 'rev-parse', 'HEAD')
    interrupted = campaign.state.get_merge_request(request['id'])
    assert interrupted['metadata']['stage'] == 'rebasing'
    assert interrupted['head'] == source

    campaign.controller.reconcile()

    merged = campaign.state.get_merge_request(request['id'])
    assert merged['status'] == 'merged'
    assert merged['head'] == rebased


def test_reconcile_recovers_conflict_before_resolver_queue(campaign, monkeypatch):
    attempt = campaign.commit_change(
        campaign.attempt('resolver-queue-crash'), 'candidate\n')
    campaign.move_mainline('mainline\n', path='app.txt')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], attempt['head'])
    queue_resolver = campaign.controller._queue_merge_resolver
    calls = 0

    def crash_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError('crashed before resolver queue')
        return queue_resolver(*args, **kwargs)

    monkeypatch.setattr(campaign.controller, '_queue_merge_resolver', crash_once)
    with pytest.raises(RuntimeError, match='resolver queue'):
        campaign.controller.reconcile()

    interrupted = campaign.state.get_merge_request(request['id'])
    assert interrupted['metadata']['stage'] == 'resolving'
    assert explore_controller.merge_in_progress(attempt['worktree'])
    assert campaign.state.list_jobs(campaign_id='c1', role='resolver') == []

    campaign.controller.reconcile()

    resolvers = campaign.state.list_jobs(campaign_id='c1', role='resolver')
    assert calls == 2
    assert len(resolvers) == 1
    assert resolvers[0]['metadata']['merge_request_id'] == request['id']


def test_reconcile_recovers_clean_aggregate_merge_before_state_persistence(
    campaign, monkeypatch,
):
    attempt = campaign.commit_change(
        campaign.attempt('aggregate-crash'), 'candidate\n', message='first')
    Path(attempt['worktree'], 'app.txt').write_text('base\n', encoding='utf-8')
    Path(attempt['worktree'], 'feature.txt').write_text('feature\n', encoding='utf-8')
    source, _ = snapshot(attempt['worktree'], 'second')
    attempt = campaign.state.update_attempt(attempt['id'], head=source)
    target = campaign.move_mainline('mainline\n', path='app.txt')
    request = campaign.state.enqueue_merge_request('c1', attempt['id'], source)
    record = campaign.state.record_merge_head
    calls = 0

    def crash_once(*args, **kwargs):
        nonlocal calls
        metadata = args[4]
        if metadata['stage'] == 'landing' and calls == 0:
            calls += 1
            raise RuntimeError('crashed before merge persistence')
        return record(*args, **kwargs)

    monkeypatch.setattr(campaign.state, 'record_merge_head', crash_once)
    with pytest.raises(RuntimeError, match='merge persistence'):
        campaign.controller.reconcile()

    merge_head = _run(attempt['worktree'], 'rev-parse', 'HEAD')
    assert _parents(campaign.repo, merge_head) == [target, source]
    assert campaign.state.get_merge_request(request['id'])['metadata']['stage'] == (
        'merge_fallback')

    campaign.controller.reconcile()

    merged = campaign.state.get_merge_request(request['id'])
    assert merged['status'] == 'merged'
    assert merged['head'] == merge_head


def test_resolver_terminal_event_replay_after_merge_is_idempotent(
    campaign, monkeypatch,
):
    attempt = campaign.commit_change(
        campaign.attempt('resolver-replay'), 'candidate\n')
    campaign.move_mainline('mainline\n', path='app.txt')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], attempt['head'])
    campaign.controller.reconcile()
    resolver = campaign.state.list_jobs(campaign_id='c1', role='resolver')[0]
    _stage_merge_resolution(campaign, attempt)
    campaign.backend.finish(resolver['backend_job_id'], 'resolved conflict')
    complete_event = campaign.state.complete_event
    calls = 0

    def crash_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError('crashed after merge before event completion')
        return complete_event(*args, **kwargs)

    monkeypatch.setattr(campaign.state, 'complete_event', crash_once)
    campaign.controller.reconcile()

    event = campaign.state.list_events('c1', job_id=resolver['id'])[0]
    assert campaign.state.get_merge_request(request['id'])['status'] == 'merged'
    assert event['status'] == 'pending'

    campaign.controller.reconcile()

    findings = [
        item for item in campaign.state.list_findings('c1')
        if item['provenance'].get('merge_request_id') == request['id']
    ]
    assert calls == 2
    assert campaign.state.get_event(event['id'])['status'] == 'completed'
    assert campaign.state.get_campaign('c1')['generation'] == 1
    assert len(findings) == 1


def test_reconcile_recovers_fast_forward_before_state_finalize(
    campaign, monkeypatch,
):
    attempt = campaign.commit_change(campaign.attempt('finalize-crash'), 'faster\n')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], attempt['head'])
    finalize = campaign.state.finalize_merge_request
    calls = 0

    def crash_once(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError('crashed before state finalize')
        return finalize(*args, **kwargs)

    monkeypatch.setattr(campaign.state, 'finalize_merge_request', crash_once)
    with pytest.raises(RuntimeError, match='state finalize'):
        campaign.controller.reconcile()
    assert _run(campaign.mainline, 'rev-parse', 'HEAD') == attempt['head']

    campaign.controller.reconcile()

    assert calls == 2
    assert campaign.state.get_merge_request(request['id'])['status'] == 'merged'
    assert campaign.state.get_campaign('c1')['generation'] == 1


def test_campaign_runs_from_planning_through_fix_merge_and_landing(campaign):
    budgets = dict(campaign.controller.budgets, parallel=1)
    campaign.state.update_campaign('c1', budgets=budgets)
    target_head = git(campaign.repo, 'rev-parse', 'main')

    campaign.controller.reconcile()
    planner = campaign.state.list_jobs(campaign_id='c1', role='planner')[0]
    campaign.backend.finish(
        planner['backend_job_id'],
        'TASKQ_JSON: {"directions":[{"hypothesis":"batch reads"}]}')
    campaign.controller.reconcile()

    attempt = campaign.state.list_attempts('c1')[0]
    optimizer = campaign.state.list_jobs(
        campaign_id='c1', role='optimizer')[0]
    Path(attempt['worktree'], 'app.txt').write_text('faster\n', encoding='utf-8')
    campaign.backend.finish(optimizer['backend_job_id'], 'implemented batching')
    campaign.controller.reconcile()

    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    campaign.finish_fix(fix, decision='accept', reason='useful change')
    campaign.controller.reconcile()
    request = campaign.state.list_merge_requests('c1')[0]
    assert request['status'] == 'queued'
    campaign.controller.reconcile()

    merged = campaign.state.get_campaign('c1')
    assert merged['generation'] == 1
    assert merged['mainline_head'] != target_head
    assert campaign.state.get_merge_request(request['id'])['status'] == 'merged'
    assert (campaign.mainline / 'app.txt').read_text(encoding='utf-8') == 'faster\n'
    assert campaign.state.list_decisions('c1', phase='fix')[0]['decision'] == 'accept'

    campaign.state.update_campaign('c1', status='draining')
    campaign.controller.reconcile()

    completed = campaign.state.get_campaign('c1')
    assert completed['status'] == 'completed'
    assert git(campaign.repo, 'rev-parse', 'main') == merged['mainline_head']
    assert (campaign.repo / 'app.txt').read_text(encoding='utf-8') == 'faster\n'
    assert not campaign.mainline.exists()
    assert campaign.backend.unregistered == ['c1']


def test_expired_deadline_enters_landing_without_allocating(campaign):
    budgets = dict(campaign.controller.budgets, deadline=0)
    campaign.state.update_campaign('c1', budgets=budgets)

    campaign.controller.reconcile()

    assert campaign.state.get_campaign('c1')['status'] == 'completed'
    assert campaign.backend.add_calls == []


def test_diverged_target_waits_for_manual_merge_without_landing_agent(campaign):
    campaign.move_mainline('campaign\n', path='campaign.txt')
    Path(campaign.repo, 'target.txt').write_text('target\n', encoding='utf-8')
    snapshot(campaign.repo, 'target change')
    campaign.state.update_campaign('c1', status='draining')

    campaign.controller.reconcile()

    waiting = campaign.state.get_campaign('c1')
    manual = waiting['config']['manual_landing']
    assert waiting['status'] == 'waiting_to_land'
    assert manual['reason'] == 'target and campaign mainline have diverged'
    assert 'switch main' in manual['command']
    assert 'merge tq/explore/c1/mainline' in manual['command']
    assert len(campaign.state.list_outbox(
        'c1', topic='campaign.manual_landing')) == 1

    _run(campaign.repo, 'merge', campaign.controller.config['mainline_branch'])
    campaign.controller.reconcile()

    assert campaign.state.get_campaign('c1')['status'] == 'completed'


def test_zero_change_limits_disable_size_gate(campaign):
    _configure_optimization(campaign, max_files=0, max_lines=0)
    attempt = campaign.attempt('unlimited-change')
    for index in range(8):
        Path(attempt['worktree'], 'file-{}.txt'.format(index)).write_text(
            'candidate\n' * 100, encoding='utf-8')
    campaign.queue_optimizer(attempt)

    campaign.controller.reconcile()

    fix = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    assert fix['metadata']['artifacts']['limit_violation'] is False
    assert fix['metadata']['artifacts']['eligible'] is True


def test_deadline_cancels_optimizer_but_still_runs_fix(campaign):
    attempt = campaign.attempt('deadline')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    job_id = campaign.controller._queue_agent(
        'optimizer', 'optimize', attempt['worktree'],
        attempt['id'], attempt['direction_id'])
    budgets = dict(campaign.controller.budgets, deadline=0)
    campaign.state.update_campaign('c1', budgets=budgets)

    campaign.controller.reconcile()

    assert campaign.state.get_campaign('c1')['status'] == 'draining'
    assert campaign.state.get_job(job_id)['status'] == 'killed'
    fixes = campaign.state.list_jobs(campaign_id='c1', role='fix')
    assert len(fixes) == 1
    assert fixes[0]['status'] == 'queued'


def test_prompt_history_is_projected_and_bounded(campaign):
    payload = 'x' * 5000
    for index in range(35):
        direction_id = 'c1-history-{:02d}'.format(index)
        campaign.state.add_direction(
            'c1', direction_id, 'hypothesis {} {}'.format(index, payload),
            'history-fingerprint-{}'.format(index),
            metadata={'approach': payload, 'irrelevant_blob': payload})
        campaign.state.add_finding(
            'finding {} {}'.format(index, payload), 'interpreted',
            campaign_id='c1', scope=payload,
            metadata={'irrelevant_blob': payload})
        campaign.state.add_decision(
            'c1', 'accept', phase='fix',
            reason='decision {} {}'.format(index, payload),
            evidence=[payload] * 10, metadata={'irrelevant_blob': payload},
            dedupe_key='history-decision-{}'.format(index))

    memory = campaign.controller._memory()
    directions = campaign.controller._direction_history()

    for section, limit in PROMPT_HISTORY_SECTION_LIMITS.items():
        assert len(json.dumps(memory[section])) <= limit + 100
        assert any('omitted_records' in item for item in memory[section])
    assert len(json.dumps(directions)) <= PROMPT_TRIED_DIRECTIONS_LIMIT + 100
    assert any('omitted_records' in item for item in directions)
    assert 'irrelevant_blob' not in json.dumps(memory)
    assert len(campaign.state.list_findings(campaign_id='c1')) == 35


def test_fix_prompt_projects_large_artifacts_without_losing_full_state(campaign):
    attempt = campaign.commit_change(
        campaign.attempt('bounded-fix-prompt'), 'candidate\n')
    huge_diff = '@' * 100000
    artifacts = {
        'candidate_changed': True,
        'head': attempt['head'],
        'changed_paths': ['app.txt'],
        'protected_paths': [],
        'changed_lines': 2,
        'limit_violation': False,
        'diff': huge_diff,
        'action_diff': huge_diff,
        'worker_output': '#' * 50000,
        'validation': {
            'checks_passed': True,
            'checks': [{
                'command': ['check'], 'exit_code': 0,
                'stdout': '%' * 50000,
            }],
        },
    }

    campaign.controller._queue_fix(attempt, artifacts)

    job = campaign.state.list_jobs(campaign_id='c1', role='fix')[0]
    prompt = job['metadata']['response_prompt']
    assert prompt.count('@') == 24000
    assert prompt.count('#') == 4000
    assert prompt.count('%') == 800
    assert 'latest_fix_diff' not in prompt
    assert job['metadata']['artifacts']['diff'] == huge_diff
    assert job['metadata']['artifacts']['worker_output'] == '#' * 50000


def test_planner_prompt_does_not_duplicate_direction_history(campaign):
    sentinel = 'UNIQUE-DIRECTION-SENTINEL'
    campaign.state.add_direction(
        'c1', 'c1-sentinel', sentinel, 'sentinel-fingerprint',
        metadata={'hypothesis': sentinel})

    campaign.controller.reconcile()

    planner = campaign.state.list_jobs(campaign_id='c1', role='planner')[0]
    prompt = planner['metadata']['response_prompt']
    assert prompt.count(sentinel) == 1
    assert '"directions"' not in prompt.split(sentinel, 1)[0]

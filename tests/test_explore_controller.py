import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from taskq.backends import git_ref
from taskq.explore.controller import ExploreController, TERMINAL
from taskq.explore.git import git, snapshot
from taskq.explore.state import ExploreState


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
        }
        self.add_calls.append({
            'id': job_id, 'command': command, 'gpus': gpus,
            'slots': slots, **kwargs,
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
            metadata={'hypothesis': 'direction {}'.format(suffix)},
        )
        return self.state.add_attempt(
            'c1', attempt_id, direction_id, branch, worktree,
            campaign['mainline_head'], metadata={'workspace': workspace},
        )

    def commit_change(self, attempt, text):
        Path(attempt['worktree'], 'app.txt').write_text(text, encoding='utf-8')
        head, changed = snapshot(attempt['worktree'], 'candidate')
        assert changed
        return self.state.update_attempt(attempt['id'], head=head)

    def queue_review(
        self, attempt, decision='accept', *, eligible=True,
        phase='inspection', merge_request_id=None,
    ):
        job_id = self.controller._queue_agent(
            'reviewer', 'review', self.mainline,
            attempt['id'], attempt['direction_id'], control=True,
            metadata={
                'phase': phase,
                'artifacts': {'eligible': eligible},
                'merge_request_id': merge_request_id,
            },
        )
        backend_id = int(self.state.get_job(job_id)['backend_job_id'])
        output = 'TASKQ_JSON: ' + json.dumps({
            'decision': decision,
            'reason': '{} evidence'.format(decision),
            'evidence': [],
            'memory_updates': [],
            'next_direction': None,
        })
        self.backend.finish(backend_id, output)
        return job_id


def _run(cwd, *args):
    return subprocess.run(
        ['git', '-C', str(cwd), *args], check=True,
        capture_output=True, text=True,
    ).stdout.strip()


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
            'max_adjustments': 2,
            'max_agent_jobs': 20,
            'max_merges': 5,
            'deadline': time.time() + 3600,
        },
        config={
            'repo_root': str(repo),
            'work_root': str(work_root),
            'mainline_branch': mainline_branch,
            'mainline_worktree': str(mainline),
            'control_cwd': str(control_cwd),
            'heartbeat_file': str(work_root / 'heartbeat'),
            'command': ['fake-agent', '{}'],
            'checks': [],
            'score': None,
            'score_direction': None,
            'min_improvement': 0,
            'protected_paths': [],
            'max_files': 5,
            'max_lines': 300,
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
def test_terminal_mutation_dispatches_one_immediate_review(campaign, terminal_status):
    attempt = campaign.attempt(terminal_status)
    Path(attempt['worktree'], 'app.txt').write_text(
        '{}\n'.format(terminal_status), encoding='utf-8')
    job_id = campaign.controller._queue_agent(
        'optimizer', 'optimize', attempt['worktree'],
        attempt['id'], attempt['direction_id'])
    backend_id = int(campaign.state.get_job(job_id)['backend_job_id'])
    campaign.backend.finish(
        backend_id, 'completed a bounded change', terminal_status)

    campaign.controller.reconcile()

    events = campaign.state.list_events('c1')
    reviews = campaign.state.list_jobs(campaign_id='c1', role='reviewer')
    assert len(events) == 1
    assert events[0]['status'] == 'completed'
    assert len(reviews) == 1
    assert reviews[0]['metadata']['artifacts']['job_status'] == terminal_status
    assert campaign.state.get_attempt(attempt['id'])['status'] == 'reviewing'


def test_mutation_retry_recognizes_snapshot_from_crashed_controller(campaign):
    attempt = campaign.attempt('snapshot-retry')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    head, changed = snapshot(attempt['worktree'], 'orphaned snapshot')
    assert changed
    assert head != attempt['head']
    job_id = campaign.controller._queue_agent(
        'optimizer', 'optimize', attempt['worktree'],
        attempt['id'], attempt['direction_id'])
    backend_id = int(campaign.state.get_job(job_id)['backend_job_id'])
    campaign.backend.finish(backend_id, 'completed a bounded change')

    campaign.controller.reconcile()

    review = campaign.state.list_jobs(campaign_id='c1', role='reviewer')[0]
    artifacts = review['metadata']['artifacts']
    assert artifacts['changed'] is True
    assert artifacts['stalled'] is False
    assert artifacts['eligible'] is True


def test_mutation_evidence_survives_review_dispatch_retry(campaign, monkeypatch):
    attempt = campaign.attempt('dispatch-retry')
    Path(attempt['worktree'], 'app.txt').write_text('candidate\n', encoding='utf-8')
    job_id = campaign.controller._queue_agent(
        'optimizer', 'optimize', attempt['worktree'],
        attempt['id'], attempt['direction_id'])
    backend_id = int(campaign.state.get_job(job_id)['backend_job_id'])
    campaign.backend.finish(backend_id, 'original worker evidence')
    dispatch = campaign.controller._dispatch_mutation_review
    calls = 0

    def fail_once(current_attempt, artifacts):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError('dispatch interrupted')
        dispatch(current_attempt, artifacts)

    monkeypatch.setattr(
        campaign.controller, '_dispatch_mutation_review', fail_once)

    campaign.controller.reconcile()
    event = campaign.state.list_events('c1')[0]
    saved = event['payload']['mutation_artifacts']
    assert event['status'] == 'pending'
    assert saved['worker_output'] == 'original worker evidence'

    campaign.controller.reconcile()

    event = campaign.state.get_event(event['id'])
    reviews = campaign.state.list_jobs(campaign_id='c1', role='reviewer')
    assert event['status'] == 'completed'
    assert event['payload']['mutation_artifacts'] == saved
    assert len(reviews) == 1
    assert reviews[0]['metadata']['artifacts']['worker_output'] == (
        'original worker evidence')


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


def test_system_gate_overrides_reviewer_accept(campaign):
    config = dict(campaign.controller.config)
    config['protected_paths'] = ['app.txt']
    campaign.state.update_campaign('c1', config=config)
    attempt = campaign.attempt('protected')
    Path(attempt['worktree'], 'app.txt').write_text('shortcut\n', encoding='utf-8')
    job_id = campaign.controller._queue_agent(
        'optimizer', 'optimize', attempt['worktree'],
        attempt['id'], attempt['direction_id'])
    backend_id = int(campaign.state.get_job(job_id)['backend_job_id'])
    campaign.backend.finish(backend_id, 'changed protected input')
    campaign.controller.reconcile()
    review = campaign.state.list_jobs(campaign_id='c1', role='reviewer')[0]
    campaign.backend.finish(
        review['backend_job_id'],
        'TASKQ_JSON: {"decision":"accept","reason":"looks fast"}',
    )

    campaign.controller.reconcile()

    decision = campaign.state.list_decisions('c1')[0]
    assert decision['decision'] == 'adjust'
    assert 'system-controlled acceptance gate' in decision['reason']
    assert campaign.state.list_merge_requests('c1') == []


def test_invalid_reviewer_gets_one_schema_repair_turn(campaign):
    attempt = campaign.attempt('repair')
    first_id = campaign.controller._queue_agent(
        'reviewer', 'review', campaign.mainline,
        attempt['id'], attempt['direction_id'], control=True,
        metadata={'phase': 'inspection', 'artifacts': {'eligible': True}},
    )
    first = campaign.state.get_job(first_id)
    campaign.backend.finish(first['backend_job_id'], 'not json')

    campaign.controller.reconcile()

    reviews = campaign.state.list_jobs(campaign_id='c1', role='reviewer')
    assert len(reviews) == 2
    assert reviews[-1]['metadata']['repair_count'] == 1
    assert campaign.state.list_decisions('c1') == []


def test_accepts_are_enqueued_in_review_completion_order(campaign):
    first = campaign.attempt('first')
    second = campaign.attempt('second')
    campaign.queue_review(first)
    campaign.queue_review(second)

    campaign.controller.reconcile()
    campaign.controller.reconcile()

    requests = campaign.state.list_merge_requests('c1')
    assert [item['attempt_id'] for item in requests] == [first['id'], second['id']]
    assert [item['accepted_seq'] for item in requests] == [1, 2]


def test_merge_barrier_prevents_planner_allocation(campaign):
    attempt = campaign.attempt('barrier')
    request = campaign.state.enqueue_merge_request('c1', attempt['id'], attempt['head'])
    assert campaign.state.claim_merge_request('c1', 'busy')['id'] == request['id']

    campaign.controller.reconcile()

    assert campaign.state.list_jobs(campaign_id='c1', role='planner') == []
    assert campaign.backend.add_calls == []


def test_planner_finishing_behind_merge_barrier_does_not_start_attempt(campaign):
    planner_id = campaign.controller._queue_agent(
        'planner', 'plan', campaign.mainline, control=True,
        metadata={'direction_count': 1})
    planner = campaign.state.get_job(planner_id)
    queued = campaign.attempt('queued-after-plan')
    campaign.state.enqueue_merge_request('c1', queued['id'], queued['head'])
    campaign.backend.finish(
        planner['backend_job_id'],
        'TASKQ_JSON: {"directions":[{"hypothesis":"batch reads"}]}',
    )

    campaign.controller.reconcile()

    direction = campaign.state.list_directions('c1')[-1]
    assert direction['hypothesis'] == 'batch reads'
    assert direction['status'] == 'deferred'
    assert campaign.state.list_attempts(direction_id=direction['id']) == []
    assert campaign.state.list_jobs(campaign_id='c1', role='optimizer') == []


def test_clean_rebase_review_fast_forward_and_cleanup(campaign):
    attempt = campaign.commit_change(campaign.attempt('merge'), 'faster\n')
    request = campaign.state.enqueue_merge_request('c1', attempt['id'], attempt['head'])

    campaign.controller.reconcile()

    review = campaign.state.list_jobs(campaign_id='c1', role='reviewer')[0]
    assert review['metadata']['phase'] == 'merge'
    campaign.backend.finish(
        review['backend_job_id'],
        'TASKQ_JSON: {"decision":"accept","reason":"verified improvement"}',
    )
    campaign.controller.reconcile()

    merged = campaign.state.get_merge_request(request['id'])
    updated = campaign.state.get_campaign('c1')
    assert merged['status'] == 'merged'
    assert updated['generation'] == 1
    assert git(campaign.mainline, 'rev-parse', 'HEAD') == attempt['head']
    assert (campaign.mainline / 'app.txt').read_text(encoding='utf-8') == 'faster\n'
    assert not Path(attempt['worktree']).exists()
    assert git(
        campaign.repo, 'show-ref', '--verify',
        'refs/heads/{}'.format(attempt['branch']), check=False,
    ) == ''


def test_reconcile_recovers_git_fast_forward_before_state_finalize(campaign):
    attempt = campaign.commit_change(campaign.attempt('recover'), 'faster\n')
    request = campaign.state.enqueue_merge_request(
        'c1', attempt['id'], attempt['head'])
    campaign.state.claim_merge_request('c1', 'crashed-controller')
    campaign.state.update_merge_request(request['id'], metadata={
        'stage': 'merging', 'expected_head': attempt['head'],
        'reason': 'accepted before crash', 'review_artifacts': {},
    })
    _run(campaign.mainline, 'merge', '--ff-only', attempt['branch'])

    campaign.controller.reconcile()

    assert campaign.state.get_merge_request(request['id'])['status'] == 'merged'
    assert campaign.state.get_campaign('c1')['mainline_head'] == attempt['head']
    assert campaign.state.get_campaign('c1')['generation'] == 1


def test_campaign_runs_from_planning_through_landing(campaign):
    budgets = dict(campaign.controller.budgets)
    budgets['parallel'] = 1
    campaign.state.update_campaign('c1', budgets=budgets)
    target_head = git(campaign.repo, 'rev-parse', 'main')

    campaign.controller.reconcile()
    planner = campaign.state.list_jobs(campaign_id='c1', role='planner')[0]
    campaign.backend.finish(
        planner['backend_job_id'],
        'TASKQ_JSON: {"directions":[{"hypothesis":"batch reads"}]}',
    )
    campaign.controller.reconcile()

    attempt = campaign.state.list_attempts('c1')[0]
    optimizer = campaign.state.list_jobs(
        campaign_id='c1', role='optimizer')[0]
    Path(attempt['worktree'], 'app.txt').write_text(
        'faster\n', encoding='utf-8')
    campaign.backend.finish(optimizer['backend_job_id'], 'implemented batching')
    campaign.controller.reconcile()

    inspection = campaign.state.list_jobs(
        campaign_id='c1', role='reviewer')[0]
    assert inspection['metadata']['phase'] == 'inspection'
    campaign.backend.finish(
        inspection['backend_job_id'],
        'TASKQ_JSON: {"decision":"accept","reason":"useful change"}',
    )
    campaign.controller.reconcile()

    request = campaign.state.list_merge_requests('c1')[0]
    assert request['status'] == 'queued'
    assert request['accepted_seq'] == 1
    campaign.controller.reconcile()

    merge_review = campaign.state.list_jobs(
        campaign_id='c1', role='reviewer')[-1]
    assert merge_review['id'] != inspection['id']
    assert merge_review['metadata']['phase'] == 'merge'
    assert merge_review['metadata']['merge_request_id'] == request['id']
    campaign.backend.finish(
        merge_review['backend_job_id'],
        'TASKQ_JSON: {"decision":"accept","reason":"rebased result holds"}',
    )
    campaign.controller.reconcile()

    merged = campaign.state.get_campaign('c1')
    assert merged['generation'] == 1
    assert merged['mainline_head'] != target_head
    assert campaign.state.get_merge_request(request['id'])['status'] == 'merged'
    assert (campaign.mainline / 'app.txt').read_text(encoding='utf-8') == 'faster\n'
    assert not Path(attempt['worktree']).exists()

    campaign.state.update_campaign('c1', status='draining')
    campaign.controller.reconcile()
    landing_review = campaign.state.list_jobs(
        campaign_id='c1', role='landing_reviewer')[0]
    campaign.backend.finish(
        landing_review['backend_job_id'],
        'TASKQ_JSON: {"decision":"accept","reason":"ready to land"}',
    )
    campaign.controller.reconcile()
    campaign.controller.reconcile()

    completed = campaign.state.get_campaign('c1')
    assert completed['status'] == 'completed'
    assert git(campaign.repo, 'rev-parse', 'main') == merged['mainline_head']
    assert (campaign.repo / 'app.txt').read_text(encoding='utf-8') == 'faster\n'
    assert not campaign.mainline.exists()
    assert git(
        campaign.repo, 'show-ref', '--verify',
        'refs/heads/{}'.format(completed['mainline_ref']), check=False,
    ) == ''
    assert campaign.backend.unregistered == ['c1']
    assert [item['phase'] for item in campaign.state.list_decisions('c1')] == [
        'inspection', 'merge', 'landing',
    ]


def test_conflict_agent_cannot_pass_review_by_aborting_rebase(campaign):
    attempt = campaign.commit_change(campaign.attempt('conflict'), 'candidate\n')
    (campaign.mainline / 'app.txt').write_text('mainline\n', encoding='utf-8')
    mainline_head, _ = snapshot(campaign.mainline, 'mainline moved')
    campaign.state.update_campaign('c1', mainline_head=mainline_head)
    request = campaign.state.enqueue_merge_request('c1', attempt['id'], attempt['head'])

    campaign.controller.reconcile()

    rebase_job = campaign.state.list_jobs(campaign_id='c1', role='rebase')[0]
    _run(attempt['worktree'], 'rebase', '--abort')
    campaign.backend.finish(rebase_job['backend_job_id'], 'resolved conflicts')
    campaign.controller.reconcile()

    assert campaign.state.get_merge_request(request['id'])['status'] == 'rejected'
    assert campaign.state.list_jobs(campaign_id='c1', role='reviewer') == []


def test_adjustment_is_deferred_while_merge_queue_is_nonempty(campaign):
    queued = campaign.attempt('queued')
    campaign.state.enqueue_merge_request('c1', queued['id'], queued['head'])
    candidate = campaign.attempt('adjust')
    campaign.queue_review(candidate, decision='adjust')

    campaign.controller.reconcile()

    assert campaign.state.get_attempt(candidate['id'])['status'] == 'deferred'
    assert campaign.state.list_jobs(campaign_id='c1', role='adjust') == []


@pytest.mark.parametrize('budget_update', [
    {'max_agent_jobs': 0},
    {'max_merges': 0},
    {'deadline': 0},
])
def test_exhausted_budget_enters_draining_without_allocating(campaign, budget_update):
    budgets = dict(campaign.controller.budgets)
    budgets.update(budget_update)
    campaign.state.update_campaign('c1', budgets=budgets)

    campaign.controller.reconcile()

    status = campaign.state.get_campaign('c1')['status']
    if 'deadline' in budget_update:
        assert status == 'landing'
        assert campaign.backend.add_calls[-1]['metadata']['role'] == 'landing_reviewer'
    else:
        assert status == 'draining'
        assert campaign.backend.add_calls == []


def test_budget_too_small_for_worker_review_and_landing_drains(campaign):
    budgets = dict(campaign.controller.budgets)
    budgets['max_agent_jobs'] = 2
    campaign.state.update_campaign('c1', budgets=budgets)

    campaign.controller.reconcile()

    assert campaign.state.get_campaign('c1')['status'] == 'draining'
    assert campaign.backend.add_calls == []


def test_deadline_cancels_queued_speculation_but_keeps_reserved_review(campaign):
    attempt = campaign.attempt('deadline')
    job_id = campaign.controller._queue_agent(
        'optimizer', 'optimize', attempt['worktree'],
        attempt['id'], attempt['direction_id'])
    budgets = dict(campaign.controller.budgets)
    budgets['deadline'] = 0
    campaign.state.update_campaign('c1', budgets=budgets)

    campaign.controller.reconcile()

    assert campaign.state.get_campaign('c1')['status'] == 'draining'
    assert campaign.state.get_job(job_id)['status'] == 'killed'
    reviews = campaign.state.list_jobs(campaign_id='c1', role='reviewer')
    assert len(reviews) == 1
    assert reviews[0]['status'] == 'queued'

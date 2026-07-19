import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

import taskq.explore.state as state_module
from taskq.explore.state import ExploreState, SCHEMA_VERSION


@pytest.fixture
def state(tmp_path):
    with ExploreState(tmp_path / 'state.sqlite') as store:
        yield store


def add_campaign(state, campaign_id='c1'):
    return state.create_campaign(
        campaign_id,
        'make it faster',
        'refs/heads/main',
        'refs/heads/tq/explore/{}/mainline'.format(campaign_id),
        target_head='abc',
        budgets={'parallel': 4, 'max_accepted_attempts': 6},
        config={
            'runner': ['codex', 'exec', '{}'],
            'checks': ['pytest -q'],
            'protected': ['tests/**'],
        },
    )


def add_attempt(state, campaign_id='c1', suffix='1'):
    direction_id = 'd{}'.format(suffix)
    attempt_id = 'a{}'.format(suffix)
    state.add_direction(
        campaign_id, direction_id, 'try {}'.format(suffix),
        'fingerprint-{}'.format(suffix),
    )
    state.add_attempt(
        campaign_id, attempt_id, direction_id,
        'refs/heads/tq/{}'.format(attempt_id), '/tmp/{}'.format(attempt_id),
        'abc',
    )
    return attempt_id


def test_campaign_state_persists_with_schema_and_wal(tmp_path):
    path = tmp_path / 'explore' / 'state.sqlite'
    state = ExploreState(path)
    created = add_campaign(state)
    state.update_campaign('c1', generation=2, status='paused')
    state.close()

    with ExploreState(path) as reopened:
        campaign = reopened.get_campaign('c1')
        assert reopened.schema_version == SCHEMA_VERSION
        assert reopened.journal_mode == 'wal'
        assert campaign['objective'] == created['objective']
        assert campaign['generation'] == 2
        assert campaign['status'] == 'paused'
        assert campaign['budgets']['parallel'] == 4
        assert campaign['config']['runner'] == ['codex', 'exec', '{}']


def test_old_state_schema_must_be_recreated(tmp_path):
    path = tmp_path / 'state.sqlite'
    with ExploreState(path) as state:
        add_campaign(state)

    with sqlite3.connect(path) as db:
        db.execute('PRAGMA user_version = 1')

    with pytest.raises(RuntimeError, match='unsupported.*recreate'):
        ExploreState(path)


def test_concurrent_openers_lock_before_reading_schema_version(
    tmp_path, monkeypatch,
):
    path = tmp_path / 'state.sqlite'
    real_connect = sqlite3.connect
    connections = []

    class RacingConnection(sqlite3.Connection):
        def execute(self, sql, parameters=(), /):
            normalized = ' '.join(sql.split()).upper()
            self.statements.append(normalized)
            return super().execute(sql, parameters)

    def connect(*args, **kwargs):
        kwargs['factory'] = RacingConnection
        connection = real_connect(*args, **kwargs)
        connection.statements = []
        connections.append(connection)
        return connection

    monkeypatch.setattr(state_module.sqlite3, 'connect', connect)

    def open_state():
        with ExploreState(path) as state:
            return state.schema_version

    with ThreadPoolExecutor(max_workers=2) as executor:
        versions = list(executor.map(lambda _: open_state(), range(2)))

    assert versions == [SCHEMA_VERSION, SCHEMA_VERSION]
    assert len(connections) == 2
    for connection in connections:
        begin = connection.statements.index('BEGIN IMMEDIATE')
        version = connection.statements.index('PRAGMA USER_VERSION')
        assert begin < version
    monkeypatch.setattr(state_module.sqlite3, 'connect', real_connect)
    with ExploreState(path) as reopened:
        assert reopened.schema_version == SCHEMA_VERSION


def test_terminal_events_are_idempotent_and_claimed_once(state):
    add_campaign(state)
    attempt_id = add_attempt(state)
    state.add_job('c1', 'job-1', 'optimizer', attempt_id=attempt_id)

    first = state.add_terminal_event('job-1', 'success', {'output': 'one'})
    duplicate = state.add_terminal_event('job-1', 'success', {'output': 'two'})

    assert duplicate == first
    assert len(state.list_events('c1')) == 1
    assert len(state.list_outbox('c1', topic='explore.terminal')) == 1

    claimed = state.claim_event('worker-1', 'c1')
    assert claimed['id'] == first['id']
    assert claimed['claimed_by'] == 'worker-1'
    assert state.claim_event('worker-2', 'c1') is None

    state.complete_event(first['id'])
    assert state.get_job('job-1')['inspected_at'] is not None
    assert state.counts('c1')['terminal_events_completed'] == 1


def test_terminal_orphan_job_and_event_are_inserted_together(state):
    add_campaign(state)
    state.add_job(
        'c1', 'orphan-1', 'planner', status='success',
        terminal_payload={'adopted': True})

    job = state.get_job('orphan-1')
    events = state.list_events('c1')
    assert job['terminal_at'] is not None
    assert len(events) == 1
    assert events[0]['payload']['adopted'] is True


def test_expired_event_lease_can_be_reclaimed(state):
    add_campaign(state)
    attempt_id = add_attempt(state)
    state.add_job('c1', 'job-1', 'optimizer', attempt_id=attempt_id)
    state.add_terminal_event('job-1', 'failed')
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    state.claim_event('stale', 'c1', lease_seconds=1, now=now)
    reclaimed = state.claim_event(
        'replacement', 'c1', now=now + timedelta(seconds=2))

    assert reclaimed['claimed_by'] == 'replacement'


def test_merge_requests_are_idempotent_and_fifo(state):
    add_campaign(state)
    first_attempt = add_attempt(state, suffix='1')
    second_attempt = add_attempt(state, suffix='2')
    state.update_attempt(first_attempt, head='head-1')
    state.update_attempt(second_attempt, head='head-2')

    first = state.enqueue_merge_request('c1', first_attempt, 'head-1')
    second = state.enqueue_merge_request('c1', second_attempt, 'head-2')
    duplicate = state.enqueue_merge_request('c1', first_attempt, 'head-1')

    assert duplicate['id'] == first['id']
    assert [first['accepted_seq'], second['accepted_seq']] == [1, 2]
    assert not state.merge_queue_empty('c1')
    assert state.claim_merge_request('c1', 'merger')['id'] == first['id']
    assert state.claim_merge_request('c1', 'other-merger') is None
    state.finalize_merge_request(first['id'], 'head-1')
    assert state.claim_merge_request('c1', 'merger')['id'] == second['id']
    state.abandon_merge_request(second['id'], 'rejected')
    assert state.merge_queue_empty('c1')


@pytest.mark.parametrize('status', ['rejected', 'failed', 'cancelled'])
def test_abandon_merge_atomically_finishes_request_attempt_and_direction(
    state, status,
):
    add_campaign(state)
    attempt_id = add_attempt(state)
    request = state.enqueue_merge_request('c1', attempt_id, 'abc')
    state.claim_merge_request('c1', 'merger')

    abandoned = state.abandon_merge_request(
        request['id'], status, {'reason': 'cannot land'})
    replay = state.abandon_merge_request(
        request['id'], status, {'reason': 'ignored on exact replay'})

    assert abandoned == replay
    assert abandoned['status'] == status
    assert abandoned['result'] == {'reason': 'cannot land'}
    assert abandoned['claimed_by'] is None
    assert abandoned['claimed_at'] is None
    assert abandoned['claim_expires_at'] is None
    assert state.get_attempt(attempt_id)['status'] == 'abandoned'
    assert state.get_direction('d1')['status'] == 'abandoned'
    assert state.merge_queue_empty('c1')
    completed = state.list_audit('c1', kind='merge.completed')
    assert len(completed) == 1
    assert completed[0]['payload']['status'] == status


def test_abandon_merge_rejects_incompatible_terminal_state_atomically(state):
    add_campaign(state)
    attempt_id = add_attempt(state)
    request = state.enqueue_merge_request('c1', attempt_id, 'abc')
    state.claim_merge_request('c1', 'merger')
    state.finalize_merge_request(request['id'], 'abc')

    with pytest.raises(ValueError, match='cannot be abandoned'):
        state.abandon_merge_request(request['id'], 'rejected')

    assert state.get_merge_request(request['id'])['status'] == 'merged'
    assert state.get_attempt(attempt_id)['status'] == 'merged'
    assert state.get_direction('d1')['status'] == 'accepted'


def test_record_merge_head_updates_attempt_and_request_atomically(state):
    add_campaign(state)
    attempt_id = add_attempt(state)
    request = state.enqueue_merge_request('c1', attempt_id, 'abc')
    request = state.claim_merge_request('c1', 'merger')
    state.update_merge_request(request['id'], metadata={
        'stage': 'snapshotting', 'accepted_head': 'abc', 'target_head': 'base-2',
    })

    prepared = state.record_merge_head(
        request['id'], attempt_id, 'abc', 'source-1', {
            'stage': 'fast_forwarding', 'accepted_head': 'abc',
            'source_head': 'source-1', 'target_head': 'base-2',
        })

    assert prepared['head'] == 'source-1'
    assert prepared['metadata']['stage'] == 'fast_forwarding'
    assert state.get_attempt(attempt_id)['head'] == 'source-1'

    state.update_merge_request(prepared['id'], metadata=dict(
        prepared['metadata'], stage='rebasing'))
    integrated = state.record_merge_head(
        request['id'], attempt_id, 'source-1', 'rebased-1', {
            **prepared['metadata'], 'stage': 'landing',
            'integration_head': 'rebased-1',
        })
    replay = state.record_merge_head(
        request['id'], attempt_id, 'source-1', 'rebased-1', {
            **prepared['metadata'], 'stage': 'landing',
            'integration_head': 'rebased-1',
        })

    assert integrated == replay
    assert integrated['metadata']['stage'] == 'landing'
    assert state.get_attempt(attempt_id)['head'] == 'rebased-1'
    assert len(state.list_audit('c1', kind='merge.head_recorded')) == 2


def test_record_merge_head_advances_stage_when_head_is_unchanged(state):
    add_campaign(state)
    attempt_id = add_attempt(state)
    request = state.enqueue_merge_request('c1', attempt_id, 'abc')
    request = state.claim_merge_request('c1', 'merger')
    state.update_merge_request(request['id'], metadata={
        'stage': 'snapshotting', 'accepted_head': 'abc', 'target_head': 'abc',
    })

    prepared = state.record_merge_head(
        request['id'], attempt_id, 'abc', 'abc', {
            'stage': 'fast_forwarding', 'accepted_head': 'abc',
            'source_head': 'abc', 'target_head': 'abc',
        })
    replay = state.record_merge_head(
        request['id'], attempt_id, 'abc', 'abc', {
            'stage': 'fast_forwarding', 'accepted_head': 'abc',
            'source_head': 'abc', 'target_head': 'abc',
        })

    assert prepared == replay
    assert prepared['metadata']['stage'] == 'fast_forwarding'
    assert len(state.list_audit('c1', kind='merge.head_recorded')) == 1


def test_record_merge_head_rejects_stale_source_without_partial_update(state):
    add_campaign(state)
    attempt_id = add_attempt(state)
    request = state.enqueue_merge_request('c1', attempt_id, 'abc')
    request = state.claim_merge_request('c1', 'merger')
    state.update_merge_request(request['id'], metadata={
        'stage': 'rebasing', 'accepted_head': 'abc',
        'source_head': 'abc', 'target_head': 'base-2',
    })

    with pytest.raises(ValueError, match='source head changed'):
        state.record_merge_head(
            request['id'], attempt_id, 'stale', 'rebased-1', {
                'stage': 'landing', 'accepted_head': 'abc',
                'source_head': 'abc', 'integration_head': 'rebased-1',
                'target_head': 'base-2',
            })

    assert state.get_attempt(attempt_id)['head'] == 'abc'
    stored = state.get_merge_request(request['id'])
    assert stored['head'] == 'abc'
    assert stored['metadata']['stage'] == 'rebasing'
    assert state.list_audit('c1', kind='merge.head_recorded') == []


def test_record_merge_head_requires_processing_linked_request(state):
    add_campaign(state)
    attempt_id = add_attempt(state, suffix='1')
    other_attempt = add_attempt(state, suffix='2')
    request = state.enqueue_merge_request('c1', attempt_id, 'abc')
    metadata = {
        'stage': 'fast_forwarding', 'accepted_head': 'abc',
        'source_head': 'source-1', 'target_head': 'base-2',
    }

    with pytest.raises(ValueError, match='not processing'):
        state.record_merge_head(
            request['id'], attempt_id, 'abc', 'source-1', metadata)

    request = state.claim_merge_request('c1', 'merger')
    state.update_merge_request(request['id'], metadata={
        'stage': 'snapshotting', 'accepted_head': 'abc', 'target_head': 'base-2',
    })
    with pytest.raises(ValueError, match='does not belong'):
        state.record_merge_head(
            request['id'], other_attempt, 'abc', 'source-1', metadata)

    assert state.get_attempt(attempt_id)['head'] == 'abc'
    assert state.get_attempt(other_attempt)['head'] == 'abc'


def test_finalize_merge_atomically_advances_campaign_attempt_and_direction(state):
    add_campaign(state)
    attempt_id = add_attempt(state)
    state.update_attempt(attempt_id, head='head-1')
    request = state.enqueue_merge_request('c1', attempt_id, 'head-1')
    state.claim_merge_request('c1', 'merger')

    merged = state.finalize_merge_request(
        request['id'], 'head-1', campaign_config={'baseline': 7})
    duplicate = state.finalize_merge_request(request['id'], 'head-1')

    assert merged['status'] == duplicate['status'] == 'merged'
    campaign = state.get_campaign('c1')
    assert campaign['mainline_head'] == 'head-1'
    assert campaign['generation'] == 1
    assert campaign['config'] == {'baseline': 7}
    assert state.get_attempt(attempt_id)['status'] == 'merged'
    assert state.get_direction('d1')['status'] == 'accepted'

    with pytest.raises(ValueError, match='does not match merge request'):
        state.finalize_merge_request(request['id'], 'wrong-head')


def test_finalize_merge_requires_processing_request_and_exact_attempt(state):
    add_campaign(state)
    attempt_id = add_attempt(state)
    state.update_attempt(attempt_id, head='head-1')
    request = state.enqueue_merge_request('c1', attempt_id, 'head-1')

    with pytest.raises(ValueError, match='not processing'):
        state.finalize_merge_request(request['id'], 'head-1')

    state.claim_merge_request('c1', 'merger')
    state.update_attempt(attempt_id, head='changed-after-queue')
    with pytest.raises(ValueError, match='not eligible'):
        state.finalize_merge_request(request['id'], 'head-1')

    assert state.get_campaign('c1')['mainline_head'] == 'abc'
    assert state.get_campaign('c1')['generation'] == 0
    assert state.get_merge_request(request['id'])['status'] == 'processing'
    assert state.get_direction('d1')['status'] == 'planned'


def test_finalize_merge_cannot_resurrect_terminal_request(state):
    add_campaign(state)
    attempt_id = add_attempt(state)
    request = state.enqueue_merge_request('c1', attempt_id, 'abc')
    state.claim_merge_request('c1', 'merger')
    state.abandon_merge_request(request['id'], 'rejected')

    with pytest.raises(ValueError, match='not processing'):
        state.finalize_merge_request(request['id'], 'abc')

    assert state.get_merge_request(request['id'])['status'] == 'rejected'
    assert state.get_attempt(attempt_id)['status'] == 'abandoned'
    assert state.get_campaign('c1')['generation'] == 0


def test_heartbeat_and_stale_campaign_detection(state):
    add_campaign(state)
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    heartbeat = state.heartbeat('c1', 'controller-7', at)

    assert heartbeat['controller_id'] == 'controller-7'
    assert state.list_stale_campaigns(at - timedelta(seconds=1)) == []
    assert [row['id'] for row in state.list_stale_campaigns(
        at + timedelta(seconds=30))] == ['c1']


def test_findings_are_project_wide_and_keep_provenance(state):
    add_campaign(state, 'c1')
    add_campaign(state, 'c2')
    state.add_finding(
        'batching stats helped', 'confirmed', campaign_id='c1',
        outcome='success', source_commit='111', confidence=.95,
        provenance={'source': 'post-merge-check'}, dedupe_key='batch-stats',
    )
    state.add_finding(
        'global cache broke isolation', 'reviewer', campaign_id='c2',
        outcome='failure', source_commit='222',
        provenance={'source': 'review'}, dedupe_key='global-cache',
    )

    findings = state.list_findings()
    assert {finding['campaign_id'] for finding in findings} == {'c1', 'c2'}
    confirmed = state.list_findings(trust='confirmed')
    assert confirmed[0]['provenance']['source'] == 'post-merge-check'
    assert state.add_finding(
        'ignored duplicate text', 'confirmed', campaign_id='c2',
        dedupe_key='batch-stats',
    )['campaign_id'] == 'c1'


def test_emit_writes_audit_and_deduplicated_outbox_atomically(state):
    add_campaign(state)
    result = state.emit(
        'c1', 'campaign.wake', {'reason': 'slot-free'},
        outbox_topic='explore.wake', dedupe_key='wake-1',
    )
    again = state.emit(
        'c1', 'campaign.wake', {'reason': 'slot-free'},
        outbox_topic='explore.wake', dedupe_key='wake-1',
    )

    assert result['outbox']['id'] == again['outbox']['id']
    assert len(state.list_outbox('c1', topic='explore.wake')) == 1
    assert len(state.list_audit('c1', kind='campaign.wake')) == 2


def test_direction_fingerprints_are_unique_per_campaign(state):
    add_campaign(state)
    state.add_direction('c1', 'd1', 'one', 'same')
    with pytest.raises(sqlite3.IntegrityError):
        state.add_direction('c1', 'd2', 'two', 'same')


def test_delete_campaigns_removes_finished_history_but_keeps_project_memory(state):
    add_campaign(state)
    attempt_id = add_attempt(state)
    state.add_job('c1', 'job-1', 'optimizer', attempt_id=attempt_id)
    state.add_terminal_event('job-1', 'success')
    state.add_finding(
        'campaign finding', 'confirmed', campaign_id='c1',
        dedupe_key='campaign-finding')
    state.add_finding(
        'project finding', 'confirmed', dedupe_key='project-finding')
    state.update_campaign('c1', status='completed')

    assert state.delete_campaigns(['c1']) == 1

    assert state.get_campaign('c1') is None
    assert state.list_jobs(campaign_id='c1') == []
    assert [item['claim'] for item in state.list_findings()] == ['project finding']


def test_delete_campaigns_refuses_active_history(state):
    add_campaign(state)

    with pytest.raises(ValueError, match='active campaigns'):
        state.delete_campaigns(['c1'])

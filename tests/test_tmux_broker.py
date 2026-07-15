import json
import os
import subprocess
import threading
import time
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from taskq.backends.tmux import broker, lifecycle


def write_meta(root, job_id, **overrides):
    job_dir = Path(root) / 'jobs' / str(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    wrapper = job_dir / 'run.sh'
    wrapper.write_text('#!/bin/sh\n', encoding='utf-8')
    meta = {
        'id': job_id,
        'command': f'job {job_id}',
        'status': 'queued',
        'exitcode': None,
        'slots_required': 1,
        'gpus_required': 0,
        'gpu_ids': '',
        'enqueue_time': '2024-01-01T00:00:00',
        'start_time': None,
        'end_time': None,
        'output_file': str(job_dir / 'output.log'),
        'wrapper': str(wrapper),
        'start_file': str(job_dir / 'start'),
        'session': f'session-{job_id}',
        'pid': None,
        'cwd': str(job_dir),
    }
    meta.update(overrides)
    path = job_dir / 'meta.json'
    path.write_text(json.dumps(meta), encoding='utf-8')
    return path


@pytest.fixture
def broker_args(tmp_path):
    return Namespace(
        state_dir=str(tmp_path),
        prefix='taskq-test',
        command='tmux',
        config_file=str(tmp_path / 'tmux.conf'),
        socket='taskq',
        socket_path=None,
        slots=2,
        history_limit=100,
        interval=1,
        gpu_free_perc=90,
        visible_gpus=None,
    )


@pytest.fixture
def fake_tmux(monkeypatch):
    calls = []
    sessions = set()

    def fake_session_exists(args, session):
        return session in sessions

    def fake_pane_pid(args, session):
        return 1234 if session in sessions else None

    def fake_tmux(args, *tmux_args, **kwargs):
        calls.append(tuple(str(arg) for arg in tmux_args))
        if tmux_args[:2] == ('new-session', '-d'):
            sessions.add(str(tmux_args[3]))
        return ''

    monkeypatch.setattr(broker, 'session_exists', fake_session_exists)
    monkeypatch.setattr(broker, 'pane_pid', fake_pane_pid)
    monkeypatch.setattr(broker, 'tmux', fake_tmux)
    return calls, sessions


def read_meta(path):
    return json.loads(Path(path).read_text())


def test_atomic_json_concurrent_writers_use_distinct_temporary_files(
    tmp_path, monkeypatch,
):
    path = tmp_path / 'controller.json'
    barrier = threading.Barrier(2)
    temporary_paths = []
    original_replace = lifecycle.os.replace

    def synchronized_replace(source, destination):
        temporary_paths.append(Path(source))
        barrier.wait(timeout=5)
        original_replace(source, destination)

    monkeypatch.setattr(lifecycle.os, 'replace', synchronized_replace)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(lifecycle.atomic_json, path, {'writer': writer})
            for writer in (1, 2)
        ]
        for future in futures:
            future.result(timeout=5)

    assert len(set(temporary_paths)) == 2
    assert json.loads(path.read_text(encoding='utf-8')) in (
        {'writer': 1}, {'writer': 2})
    assert not any(temporary.exists() for temporary in temporary_paths)


def test_command_result_requires_matching_submission_for_uuid_jobs(tmp_path):
    path = tmp_path / 'command-result.json'
    meta = {
        'submission_id': 'current-submission',
        'command_result_file': str(path),
    }
    for payload in (
        {'exitcode': 0},
        {'exitcode': 0, 'submission_id': 'old-submission'},
    ):
        path.write_text(json.dumps(payload), encoding='utf-8')
        assert lifecycle.command_result(meta) is None

    path.write_text(json.dumps({
        'exitcode': 0,
        'submission_id': 'current-submission',
    }), encoding='utf-8')
    assert lifecycle.command_result(meta)['exitcode'] == 0


def test_refresh_merge_recovers_only_explicit_cancelled_merging_parent(tmp_path):
    status_file = tmp_path / 'merge-status.json'
    status_file.write_text(json.dumps({
        'state': 'cancelled',
        'stage': 'cancelled',
        'cancelled': True,
        'submission_id': 'submission-1',
    }), encoding='utf-8')
    meta = {
        'status': 'merging',
        'exitcode': None,
        'end_time': None,
        'pid': 123,
        'submission_id': 'submission-1',
        'merge': {'stage': 'staged', 'submission_id': 'submission-1'},
        'merge_status_file': str(status_file),
        'merge_result_file': str(tmp_path / 'missing-result.json'),
    }

    assert lifecycle.refresh_merge(meta, '2024-01-01T00:00:02') is True
    assert meta['status'] == 'killed'
    assert meta['exitcode'] == -1
    assert meta['end_time'] == '2024-01-01T00:00:02'
    assert meta['pid'] is None
    assert meta['merge']['stage'] == 'cancelled'

    failed = dict(meta, status='failed', exitcode=7)
    failed['merge'] = {'stage': 'staged'}
    lifecycle.refresh_merge(failed, '2024-01-01T00:00:03')
    assert failed['status'] == 'failed'
    assert failed['exitcode'] == 7


@pytest.mark.parametrize('sidecar_submission', [None, 'old-submission'])
def test_refresh_merge_rejects_unowned_terminal_sidecar(
    tmp_path, sidecar_submission,
):
    result_file = tmp_path / 'merge-result.json'
    projection = {'stage': 'landed'}
    if sidecar_submission is not None:
        projection['submission_id'] = sidecar_submission
    result_file.write_text(json.dumps({
        'status': 'success',
        'exitcode': 0,
        'merge': projection,
    }), encoding='utf-8')
    meta = {
        'status': 'merging',
        'exitcode': None,
        'submission_id': 'current-submission',
        'merge': {
            'stage': 'staged',
            'submission_id': 'current-submission',
        },
        'merge_result_file': str(result_file),
    }

    assert lifecycle.refresh_merge(meta, '2024-01-01T00:00:02') is False
    assert meta['status'] == 'merging'
    assert meta['exitcode'] is None
    assert meta['merge']['stage'] == 'staged'


def test_broker_starts_jobs_within_slots(broker_args, fake_tmux, monkeypatch):
    calls, _ = fake_tmux
    p1 = write_meta(broker_args.state_dir, 1, slots_required=2)
    p2 = write_meta(broker_args.state_dir, 2, slots_required=1)
    monkeypatch.setattr(broker, 'query_free_gpus', lambda args: [])
    broker.tick(broker_args)
    assert read_meta(p1)['status'] == 'running'
    assert read_meta(p2)['status'] == 'queued'
    assert sum(1 for call in calls if call[:2] == ('new-session', '-d')) == 1


def test_broker_oversubscribes_single_large_job(broker_args, fake_tmux, monkeypatch):
    broker_args.slots = 1
    path = write_meta(broker_args.state_dir, 1, slots_required=3)
    monkeypatch.setattr(broker, 'query_free_gpus', lambda args: [])
    broker.tick(broker_args)
    assert read_meta(path)['status'] == 'running'


def test_broker_zero_slot_jobs_run_without_consuming_capacity(
    broker_args, fake_tmux, monkeypatch
):
    full = write_meta(broker_args.state_dir, 1, slots_required=2)
    control = write_meta(broker_args.state_dir, 2, slots_required=0)
    waiting = write_meta(broker_args.state_dir, 3, slots_required=1)
    monkeypatch.setattr(broker, 'query_free_gpus', lambda args: [])

    broker.tick(broker_args)

    assert read_meta(full)['status'] == 'running'
    assert read_meta(control)['status'] == 'running'
    assert read_meta(waiting)['status'] == 'queued'


def test_broker_starts_and_restarts_registered_controllers(
    broker_args, fake_tmux, monkeypatch, tmp_path
):
    calls, sessions = fake_tmux
    controller_dir = Path(broker_args.state_dir) / 'controllers'
    controller_dir.mkdir()
    heartbeat = tmp_path / 'heartbeat'
    heartbeat.write_text('ok', encoding='utf-8')
    path = controller_dir / 'campaign.json'
    path.write_text(json.dumps({
        'name': 'campaign',
        'session': 'controller-campaign',
        'argv': ['python', '-m', 'controller', 'two words'],
        'cwd': str(tmp_path),
        'heartbeat_file': str(heartbeat),
        'timeout': 30,
        'registered_at': time.time(),
        'enabled': True,
    }), encoding='utf-8')

    broker.guard_controllers(broker_args)

    starts = [call for call in calls if call[:2] == ('new-session', '-d')]
    assert starts[0][-1] == "python -m controller 'two words'"
    assert 'controller-campaign' in sessions

    calls.clear()
    sessions.clear()
    broker.guard_controllers(broker_args)
    assert not any(call[:2] == ('new-session', '-d') for call in calls)

    calls.clear()
    sessions.add('controller-campaign')
    old = time.time() - 60
    os.utime(heartbeat, (old, old))
    meta = read_meta(path)
    meta['last_restart'] = old
    path.write_text(json.dumps(meta), encoding='utf-8')
    broker.guard_controllers(broker_args)

    assert ('kill-session', '-t', 'controller-campaign') in calls
    assert any(call[:2] == ('new-session', '-d') for call in calls)


def test_broker_uses_runtime_slots_config(broker_args, fake_tmux, monkeypatch):
    broker_args.slots = 1
    p1 = write_meta(broker_args.state_dir, 1)
    p2 = write_meta(broker_args.state_dir, 2)
    config_path = Path(broker_args.state_dir) / 'broker.json'
    config_path.write_text(json.dumps({'slots': 2}), encoding='utf-8')
    monkeypatch.setattr(broker, 'query_free_gpus', lambda args: [])

    broker.tick(broker_args)

    assert read_meta(p1)['status'] == 'running'
    assert read_meta(p2)['status'] == 'running'


def test_broker_waits_for_successful_dependencies(
    broker_args, fake_tmux, monkeypatch
):
    dep_path = write_meta(broker_args.state_dir, 1, status='queued')
    child_path = write_meta(broker_args.state_dir, 2, depends_on=[1])
    monkeypatch.setattr(broker, 'query_free_gpus', lambda args: [])

    broker.tick(broker_args)

    assert read_meta(child_path)['status'] == 'queued'

    dep_meta = read_meta(dep_path)
    dep_meta.update({'status': 'success', 'exitcode': 0})
    dep_path.write_text(json.dumps(dep_meta), encoding='utf-8')

    broker.tick(broker_args)

    assert read_meta(child_path)['status'] == 'running'


def test_broker_fails_job_when_dependency_fails(
    broker_args, fake_tmux, monkeypatch
):
    write_meta(broker_args.state_dir, 1, status='failed', exitcode=1)
    child_path = write_meta(broker_args.state_dir, 2, depends_on=[1])
    monkeypatch.setattr(broker, 'query_free_gpus', lambda args: [])

    broker.tick(broker_args)

    child = read_meta(child_path)
    assert child['status'] == 'failed'
    assert child['exitcode'] is None
    assert 'dependency 1 ended with status failed' in Path(
        child['output_file']).read_text(encoding='utf-8')


def test_broker_runtime_slots_caches_unchanged_config(broker_args, monkeypatch):
    config_path = Path(broker_args.state_dir) / 'broker.json'
    config_path.write_text(json.dumps({'slots': 3}), encoding='utf-8')
    calls = {'count': 0}
    original_read_meta = broker.read_meta

    def counting_read_meta(path):
        calls['count'] += 1
        return original_read_meta(path)

    monkeypatch.setattr(broker, 'read_meta', counting_read_meta)

    assert broker.runtime_slots(broker_args) == 3
    assert broker.runtime_slots(broker_args) == 3
    assert calls['count'] == 1


def test_broker_gpu_allocation_and_cpu_minus_one(broker_args, fake_tmux, monkeypatch):
    calls, _ = fake_tmux
    p1 = write_meta(broker_args.state_dir, 1, gpus_required=1)
    p2 = write_meta(broker_args.state_dir, 2, gpus_required=1)
    p3 = write_meta(broker_args.state_dir, 3, gpus_required=0)
    monkeypatch.setattr(broker, 'query_free_gpus', lambda args: [0])
    monkeypatch.setattr(broker.random, 'shuffle', lambda values: None)
    broker.tick(broker_args)

    assert read_meta(p1)['status'] == 'running'
    assert read_meta(p1)['gpu_ids'] == '0'
    assert read_meta(p2)['status'] == 'queued'
    assert read_meta(p3)['status'] == 'running'
    new_sessions = [call for call in calls if call[:2] == ('new-session', '-d')]
    assert any('-e' in call and 'TASKQ_GPU_IDS=0' in call for call in new_sessions)
    assert any('-e' in call and 'TASKQ_GPU_IDS=-1' in call for call in new_sessions)
    assert all(call[-1] == 'exec "$TASKQ_WRAPPER"' for call in new_sessions)
    assert all('TASKQ_GPU_IDS=' not in call[-1] for call in new_sessions)
    assert not any(call and call[0] == 'send-keys' for call in calls)
    pipe_panes = [call for call in calls if call and call[0] == 'pipe-pane']
    assert len(pipe_panes) == 2


def test_broker_releases_job_after_pipe_pane(broker_args, fake_tmux, monkeypatch):
    calls, _ = fake_tmux
    path = write_meta(broker_args.state_dir, 1)
    monkeypatch.setattr(broker, 'query_free_gpus', lambda args: [])
    broker.tick(broker_args)

    meta = read_meta(path)
    assert Path(meta['start_file']).exists()
    new_index = next(
        i for i, call in enumerate(calls)
        if call[:2] == ('new-session', '-d')
    )
    pipe_index = next(
        i for i, call in enumerate(calls)
        if call and call[0] == 'pipe-pane'
    )
    assert new_index < pipe_index


def test_broker_no_nvidia_smi_fails_gpu_job_but_runs_cpu(
    broker_args, fake_tmux, monkeypatch
):
    gpu_path = write_meta(broker_args.state_dir, 1, gpus_required=1)
    cpu_path = write_meta(broker_args.state_dir, 2, gpus_required=0)
    monkeypatch.setattr(broker, 'query_free_gpus', lambda args: None)
    broker.tick(broker_args)
    gpu_meta = read_meta(gpu_path)
    assert gpu_meta['status'] == 'failed'
    assert 'nvidia-smi is not available' in Path(
        gpu_meta['output_file']).read_text(encoding='utf-8')
    assert read_meta(cpu_path)['status'] == 'running'


def test_query_free_gpus_filters_visibility_and_threshold(monkeypatch, broker_args):
    broker_args.visible_gpus = '1,2'
    broker_args.gpu_free_perc = 90

    class Result:
        stdout = '0, 950, 1000\n1, 950, 1000\n2, 100, 1000\n'

    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: Result())
    assert broker.query_free_gpus(broker_args) == [1]


def test_query_free_gpus_missing_nvidia_smi(monkeypatch, broker_args):
    def raise_missing(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(subprocess, 'run', raise_missing)
    assert broker.query_free_gpus(broker_args) is None


def test_query_free_gpus_no_reported_devices(monkeypatch, broker_args):
    class Result:
        stdout = ''

    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: Result())
    assert broker.query_free_gpus(broker_args) is None


def test_visible_gpus_from_environment(monkeypatch, broker_args):
    broker_args.visible_gpus = None
    monkeypatch.setenv('TS_VISIBLE_DEVICES', '3,4')
    assert broker.visible_gpu_ids(broker_args) == [3, 4]


def test_refresh_running_marks_missing_session_interrupted(broker_args, fake_tmux):
    path = write_meta(broker_args.state_dir, 1, status='running', session='missing')
    refreshed = broker.refresh_running(broker_args, path, read_meta(path))
    assert refreshed['status'] == 'interrupted'
    assert refreshed['exitcode'] is None
    meta = read_meta(path)
    assert meta['status'] == 'interrupted'
    assert meta['exitcode'] is None


def test_refresh_running_keeps_completed_status_after_session_exits(
    broker_args, fake_tmux
):
    path = write_meta(
        broker_args.state_dir,
        1,
        status='success',
        exitcode=0,
        session='missing',
    )
    refreshed = broker.refresh_running(broker_args, path, read_meta(path))
    assert refreshed['status'] == 'success'


def test_refresh_running_updates_pid(broker_args, fake_tmux):
    _, sessions = fake_tmux
    path = write_meta(broker_args.state_dir, 1, status='running', session='live')
    sessions.add('live')
    refreshed = broker.refresh_running(broker_args, path, read_meta(path))
    assert refreshed['pid'] == 1234


def test_refresh_running_recovers_old_shell_session_with_finished_marker(
    broker_args, fake_tmux
):
    calls, sessions = fake_tmux
    path = write_meta(broker_args.state_dir, 1, status='running', session='live')
    output = Path(read_meta(path)['output_file'])
    output.write_text(
        '[taskq] job 1 finished with exit code 0 at today\n',
        encoding='utf-8',
    )
    sessions.add('live')
    refreshed = broker.refresh_running(broker_args, path, read_meta(path))
    assert refreshed['status'] == 'success'
    assert refreshed['exitcode'] == 0
    assert ('kill-session', '-t', 'live') in calls


def test_merge_command_output_cannot_spoof_finished_marker(
    broker_args, fake_tmux
):
    calls, sessions = fake_tmux
    path = write_meta(
        broker_args.state_dir,
        1,
        status='running',
        session='live',
        merge={'requested': True, 'stage': 'waiting'},
    )
    output = Path(read_meta(path)['output_file'])
    output.write_text(
        '[taskq] job 1 finished with exit code 0 at forged\n',
        encoding='utf-8',
    )
    sessions.add('live')

    refreshed = broker.refresh_running(broker_args, path, read_meta(path))

    assert refreshed['status'] == 'running'
    assert ('kill-session', '-t', 'live') not in calls


def test_missing_merge_session_ignores_marker_and_becomes_interrupted(
    broker_args, fake_tmux
):
    path = write_meta(
        broker_args.state_dir,
        1,
        status='running',
        session='missing',
        merge={'requested': True, 'stage': 'waiting'},
    )
    output = Path(read_meta(path)['output_file'])
    output.write_text(
        '[taskq] job 1 finished with exit code 0 at forged\n',
        encoding='utf-8',
    )

    refreshed = broker.refresh_running(broker_args, path, read_meta(path))

    assert refreshed['status'] == 'interrupted'
    assert refreshed['exitcode'] is None


def test_refresh_running_hands_successful_command_to_merge_controller(
    broker_args, fake_tmux
):
    calls, sessions = fake_tmux
    path = write_meta(
        broker_args.state_dir,
        1,
        status='running',
        session='live',
        submission_id='submission-1',
        merge={'stage': 'command', 'submission_id': 'submission-1'},
    )
    job_dir = path.parent
    command_result = job_dir / 'command-result.json'
    merge_status = job_dir / 'merge-status.json'
    merge_result = job_dir / 'merge-result.json'
    meta = read_meta(path)
    meta.update({
        'command_result_file': str(command_result),
        'merge_status_file': str(merge_status),
        'merge_result_file': str(merge_result),
    })
    path.write_text(json.dumps(meta), encoding='utf-8')
    command_result.write_text(json.dumps({
        'exitcode': 0,
        'end_time': '2024-01-01T00:00:01',
        'submission_id': 'submission-1',
    }), encoding='utf-8')
    sessions.add('live')

    refreshed = broker.refresh_running(broker_args, path, read_meta(path))

    assert refreshed['status'] == 'merging'
    assert refreshed['exitcode'] is None
    assert refreshed['command_exitcode'] == 0
    assert refreshed['merge']['stage'] == 'command'
    assert refreshed['end_time'] is None
    assert ('kill-session', '-t', 'live') in calls

    merge_status.write_text(json.dumps({
        'merge': {
            'stage': 'staged',
            'sequence': 4,
            'submission_id': 'submission-1',
        },
    }), encoding='utf-8')
    refreshed = broker.refresh_running(broker_args, path, read_meta(path))
    assert refreshed['status'] == 'merging'
    assert refreshed['merge']['stage'] == 'staged'
    assert refreshed['merge']['sequence'] == 4

    merge_result.write_text(json.dumps({
        'status': 'success',
        'exitcode': 0,
        'end_time': '2024-01-01T00:00:02',
        'merge': {
            'stage': 'landed',
            'landed_head': 'abc123',
            'submission_id': 'submission-1',
        },
    }), encoding='utf-8')
    refreshed = broker.refresh_running(broker_args, path, read_meta(path))
    assert refreshed['status'] == 'success'
    assert refreshed['exitcode'] == 0
    assert refreshed['command_exitcode'] == 0
    assert refreshed['merge']['stage'] == 'landed'


def test_refresh_running_preserves_merge_command_failure_exitcode(
    broker_args, fake_tmux
):
    _, sessions = fake_tmux
    path = write_meta(
        broker_args.state_dir,
        1,
        status='running',
        session='live',
        merge={'requested': True, 'stage': 'waiting'},
    )
    command_result = path.parent / 'command-result.json'
    meta = read_meta(path)
    meta['command_result_file'] = str(command_result)
    path.write_text(json.dumps(meta), encoding='utf-8')
    command_result.write_text(json.dumps({
        'exitcode': 7,
        'end_time': '2024-01-01T00:00:01',
    }), encoding='utf-8')
    sessions.add('live')

    refreshed = broker.refresh_running(broker_args, path, read_meta(path))

    assert refreshed['status'] == 'failed'
    assert refreshed['exitcode'] == 7
    assert refreshed['command_exitcode'] == 7
    assert refreshed['failure_phase'] == 'command'
    assert refreshed['merge']['stage'] == 'skipped'


def test_refresh_running_recovers_cancelled_merging_parent(
    broker_args, fake_tmux
):
    path = write_meta(
        broker_args.state_dir,
        1,
        status='merging',
        submission_id='submission-1',
        merge={'stage': 'staged', 'submission_id': 'submission-1'},
    )
    status_file = path.parent / 'merge-status.json'
    status_file.write_text(json.dumps({
        'state': 'cancelled',
        'stage': 'cancelled',
        'cancelled': True,
        'submission_id': 'submission-1',
    }), encoding='utf-8')
    meta = read_meta(path)
    meta.update({
        'merge_status_file': str(status_file),
        'merge_result_file': str(path.parent / 'merge-result.json'),
    })
    path.write_text(json.dumps(meta), encoding='utf-8')

    refreshed = broker.refresh_running(broker_args, path, read_meta(path))

    assert refreshed['status'] == 'killed'
    assert refreshed['exitcode'] == -1
    assert read_meta(path)['status'] == 'killed'


def test_tick_releases_slot_when_command_moves_to_merging(
    broker_args, fake_tmux, monkeypatch
):
    _, sessions = fake_tmux
    first = write_meta(
        broker_args.state_dir,
        1,
        status='running',
        session='live',
        slots_required=2,
        merge={'stage': 'command'},
    )
    command_result = first.parent / 'command-result.json'
    first_meta = read_meta(first)
    first_meta['command_result_file'] = str(command_result)
    first.write_text(json.dumps(first_meta), encoding='utf-8')
    command_result.write_text(json.dumps({
        'exitcode': 0,
        'end_time': '2024-01-01T00:00:01',
    }), encoding='utf-8')
    second = write_meta(broker_args.state_dir, 2, slots_required=2)
    sessions.add('live')
    monkeypatch.setattr(broker, 'query_free_gpus', lambda args: [])

    broker.tick(broker_args)

    assert read_meta(first)['status'] == 'merging'
    assert read_meta(second)['status'] == 'running'


def test_start_job_does_not_overwrite_completed_metadata(
    broker_args, fake_tmux, monkeypatch
):
    _, sessions = fake_tmux
    path = write_meta(broker_args.state_dir, 1)
    original_tmux = broker.tmux

    def completing_tmux(args, *tmux_args, **kwargs):
        result = original_tmux(args, *tmux_args, **kwargs)
        if tmux_args[:2] == ('new-session', '-d'):
            meta = read_meta(path)
            meta.update({'status': 'success', 'exitcode': 0})
            path.write_text(json.dumps(meta), encoding='utf-8')
        return result

    monkeypatch.setattr(broker, 'tmux', completing_tmux)
    sessions.add('session-1')
    broker.start_job(broker_args, path, read_meta(path))
    meta = read_meta(path)
    assert meta['status'] == 'success'
    assert meta['exitcode'] == 0
    assert meta['pid'] is None


def test_start_job_marks_failed_when_tmux_new_session_fails(
    broker_args, fake_tmux, monkeypatch
):
    path = write_meta(broker_args.state_dir, 1)
    original_tmux = broker.tmux

    def failing_new_session(args, *tmux_args, **kwargs):
        if tmux_args[:2] == ('new-session', '-d'):
            raise subprocess.CalledProcessError(
                returncode=2,
                cmd='tmux new-session',
                stderr=b'bad shell syntax',
            )
        return original_tmux(args, *tmux_args, **kwargs)

    monkeypatch.setattr(broker, 'tmux', failing_new_session)
    broker.start_job(broker_args, path, read_meta(path))
    meta = read_meta(path)
    assert meta['status'] == 'failed'
    assert meta['exitcode'] == 2
    assert 'bad shell syntax' in Path(meta['output_file']).read_text()


def test_tmux_cmd_socket_path(broker_args):
    broker_args.socket_path = '/tmp/taskq.sock'
    assert broker.tmux_cmd(broker_args, 'ls') == [
        'tmux', '-f', broker_args.config_file,
        '-S', '/tmp/taskq.sock', 'ls'
    ]

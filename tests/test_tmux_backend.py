import json
import subprocess

import pytest

from taskq.backends import BACKENDS
from taskq.backends.tmux.backend import TmuxBackend
from taskq.common import FilterArgs


@pytest.fixture
def tmux_backend(monkeypatch, tmp_path):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    monkeypatch.setattr(
        'taskq.backends.base.subprocess.check_output',
        lambda command: b'',
    )
    backend = TmuxBackend(
        'tmux',
        {
            'backend': 'tmux',
            'group': 'test',
            'command': 'tmux',
            'socket': 'shared',
            'slots': 2,
            'alloc': {'gpus': 0, 'slots': 1},
            'env': {},
            'state_dir': str(tmp_path),
            'tmux_config': str(tmp_path / 'tmux.conf'),
            'history_limit': 50,
            'broker_interval': 0,
        },
    )
    calls = []
    sessions = set()

    def fake_tmux(*args, capture_output=True, check=True):
        calls.append((tuple(str(a) for a in args), capture_output, check))
        if args[:2] == ('list-sessions', '-F'):
            return '\n'.join(sorted(sessions))
        if args and args[0] == 'new-session':
            sessions.add(str(args[3]))
        if args[:3] == ('display-message', '-p', '-t'):
            return '4321'
        if args and args[0] == 'capture-pane':
            return 'pane output'
        return ''

    monkeypatch.setattr(backend, '_tmux', fake_tmux)
    monkeypatch.setattr(backend, '_session_exists', lambda session: session in sessions)
    backend.calls = calls
    backend.sessions = sessions
    return backend


def read_meta(backend, job_id):
    return json.loads((backend._job_dir(job_id) / 'meta.json').read_text())


def test_tmux_registered_and_socket_commands(tmux_backend):
    assert BACKENDS['tmux'] is TmuxBackend
    assert tmux_backend._tmux_cmd('list-sessions') == [
        'tmux', '-f', str(tmux_backend.tmux_config_file),
        '-L', 'shared', 'list-sessions'
    ]
    tmux_backend._ensure_tmux_config()
    assert 'remain-on-exit off' in tmux_backend.tmux_config_file.read_text()
    assert tmux_backend.backend_command(['list-sessions'], commit=False) is None


def test_tmux_socket_path_command(monkeypatch, tmp_path):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    backend = TmuxBackend(
        'tmux',
        {
            'backend': 'tmux', 'group': 'g', 'command': 'tmux',
            'socket_path': str(tmp_path / 'sock'), 'slots': 1,
            'alloc': {}, 'env': {}, 'state_dir': str(tmp_path),
        },
    )
    assert backend._tmux_cmd('ls') == [
        'tmux', '-f', str(backend.tmux_config_file),
        '-S', str(tmp_path / 'sock'), 'ls'
    ]


def test_tmux_add_queues_job_and_ensures_broker(tmux_backend):
    job_id = tmux_backend.add('echo hi', gpus=1, slots=2)
    meta = read_meta(tmux_backend, int(job_id))
    assert meta['status'] == 'queued'
    assert meta['gpus_required'] == 1
    assert meta['slots_required'] == 2
    assert meta['gpu_ids'] == ''
    wrapper = (tmux_backend._job_dir(int(job_id)) / 'run.sh').read_text()
    assert 'CUDA_VISIBLE_DEVICES' in wrapper
    assert 'exec "${SHELL:-/bin/sh}"' not in wrapper
    assert 'exit "$exitcode"' in wrapper
    assert f'tee -a {meta["output_file"]}' in wrapper
    assert tmux_backend.broker_session in tmux_backend.sessions


def test_tmux_restarts_old_broker(tmux_backend):
    tmux_backend.sessions.add(tmux_backend.broker_session)
    tmux_backend.add('echo hi', gpus=0, slots=1)
    killed = [call[0][2] for call in tmux_backend.calls
              if call[0][:2] == ('kill-session', '-t')]
    assert tmux_backend.broker_session in killed


def test_tmux_keeps_current_broker(tmux_backend):
    tmux_backend.sessions.add(tmux_backend.broker_session)

    def fake_version():
        return TmuxBackend.BROKER_VERSION

    tmux_backend._broker_version = fake_version
    tmux_backend.add('echo hi', gpus=0, slots=1)
    killed = [call[0][2] for call in tmux_backend.calls
              if call[0][:2] == ('kill-session', '-t')]
    assert tmux_backend.broker_session not in killed


def test_tmux_job_info_full_info_and_filters(tmux_backend):
    job_id = int(tmux_backend.add('echo hi', gpus=0, slots=1))
    info = tmux_backend.job_info(ids=[job_id], filters=FilterArgs(queued=True))
    assert info == [{'id': job_id, 'status': 'queued', 'exitcode': None}]
    full = tmux_backend.full_info(ids=[job_id], filters=FilterArgs(), tqdm_disable=True)[0]
    assert full['command'] == 'echo hi'
    assert full['status'] == 'queued'


def test_tmux_output_waits_and_attaches(monkeypatch, tmux_backend, capsys):
    job_id = int(tmux_backend.add('sleep 1', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    tmux_backend.sessions.discard(meta['session'])
    waits = {'count': 0}

    def fake_sleep(_):
        waits['count'] += 1
        tmux_backend.sessions.add(meta['session'])

    monkeypatch.setattr('taskq.backends.tmux.backend.time.sleep', fake_sleep)
    tmux_backend.output({'id': job_id}, 0, shell=True)
    assert waits['count'] == 1
    assert any(call[0][0] == 'attach-session' for call in tmux_backend.calls)
    assert 'waiting for a slot' in capsys.readouterr().out


def test_tmux_output_capture_and_file_fallback(tmux_backend):
    job_id = int(tmux_backend.add('echo hi', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    tmux_backend.sessions.add(meta['session'])
    assert tmux_backend.output({'id': job_id}, 1) == 'pane output'

    tmux_backend.sessions.discard(meta['session'])
    output_file = tmux_backend._job_dir(job_id) / 'output.log'
    output_file.write_text('a\nb\nc\n', encoding='utf-8')
    assert tmux_backend.output({'id': job_id}, 2) == 'c\n'


def test_tmux_kill_remove_and_backend_reset(tmux_backend):
    job_id = int(tmux_backend.add('echo hi', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    tmux_backend.sessions.add(meta['session'])
    tmux_backend.kill({'id': job_id})
    assert read_meta(tmux_backend, job_id)['status'] == 'killed'
    assert any(call[0][:2] == ('kill-session', '-t') for call in tmux_backend.calls)

    tmux_backend.remove({'id': job_id})
    assert not tmux_backend._job_dir(job_id).exists()

    tmux_backend.sessions.update({tmux_backend.broker_session, f'{tmux_backend.prefix}-99'})
    tmux_backend.backend_kill(None)
    killed = [call[0][2] for call in tmux_backend.calls if call[0][:2] == ('kill-session', '-t')]
    assert tmux_backend.broker_session in killed


def test_tmux_refresh_marks_missing_running_session_failed(tmux_backend):
    job_id = int(tmux_backend.add('echo hi', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    meta['status'] = 'running'
    (tmux_backend._job_dir(job_id) / 'meta.json').write_text(json.dumps(meta))
    info = tmux_backend.job_info(ids=[job_id], filters=FilterArgs())[0]
    assert info['status'] == 'failed'


def test_tmux_refresh_keeps_completed_status_after_session_exits(tmux_backend):
    job_id = int(tmux_backend.add('echo hi', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    meta.update({'status': 'success', 'exitcode': 0})
    (tmux_backend._job_dir(job_id) / 'meta.json').write_text(json.dumps(meta))
    info = tmux_backend.job_info(ids=[job_id], filters=FilterArgs())[0]
    assert info['status'] == 'success'


def test_tmux_refresh_cleans_up_stale_legacy_shell(tmux_backend):
    job_id = int(tmux_backend.add('sleep 1', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    meta.update({
        'status': 'running',
        'start_time': '2024-01-01T00:00:00',
        'pid': 1234,
    })
    (tmux_backend._job_dir(job_id) / 'run.sh').write_text(
        '#!/usr/bin/env bash\nsleep 1\nexit "$?"\n',
        encoding='utf-8',
    )
    (tmux_backend._job_dir(job_id) / 'meta.json').write_text(json.dumps(meta))
    tmux_backend.sessions.add(meta['session'])
    tmux_backend._capture_pane = lambda session, tail: '❯\n'
    tmux_backend._pane_current_command = lambda session: 'zsh'
    info = tmux_backend.job_info(ids=[job_id], filters=FilterArgs())[0]
    assert info['status'] == 'failed'
    assert meta['session'] in [
        call[0][2] for call in tmux_backend.calls
        if call[0][:2] == ('kill-session', '-t')
    ]


def test_tmux_refresh_treats_xonsh_as_stale_legacy_shell(tmux_backend):
    job_id = int(tmux_backend.add('sleep 1', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    meta.update({
        'status': 'running',
        'start_time': '2024-01-01T00:00:00',
        'pid': 1234,
    })
    (tmux_backend._job_dir(job_id) / 'run.sh').write_text(
        '#!/usr/bin/env bash\nsleep 1\nexit "$?"\n',
        encoding='utf-8',
    )
    (tmux_backend._job_dir(job_id) / 'meta.json').write_text(json.dumps(meta))
    tmux_backend.sessions.add(meta['session'])
    tmux_backend._capture_pane = lambda session, tail: '❯\n'
    tmux_backend._pane_current_command = lambda session: 'python3.11'
    info = tmux_backend.job_info(ids=[job_id], filters=FilterArgs())[0]
    assert info['status'] == 'failed'

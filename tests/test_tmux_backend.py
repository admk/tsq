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
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path))
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
        if args[:2] == ('kill-session', '-t'):
            sessions.discard(str(args[2]))
        if args[:3] == ('display-message', '-p', '-t'):
            return '4321'
        if args and args[0] == 'capture-pane':
            return 'pane output'
        return ''

    monkeypatch.setattr(backend, '_tmux', fake_tmux)
    monkeypatch.setattr(backend, '_session_exists', lambda session: session in sessions)
    backend.attached = []
    monkeypatch.setattr(
        backend, '_attach_tmux', lambda session: backend.attached.append(session)
    )
    backend.calls = calls
    backend.sessions = sessions
    return backend


def read_meta(backend, job_id):
    return json.loads((backend._job_dir(job_id) / 'meta.json').read_text())


def test_tmux_registered_and_socket_commands(tmux_backend):
    assert BACKENDS['tmux'] is TmuxBackend
    assert TmuxBackend._sanitize_name('/tmp/tq.sock') == 'tmp-tq-sock'
    assert tmux_backend._tmux_cmd('list-sessions') == [
        'tmux', '-f', str(tmux_backend.tmux_default_config_file),
        '-S', str(tmux_backend.socket_path), 'list-sessions'
    ]
    config = tmux_backend.tmux_default_config_file.read_text()
    assert 'set -g prefix None' in config
    assert 'bind-key -n C-c detach-client' in config
    assert 'bind-key -n C-b copy-mode -u' in config
    assert 'bind-key -n C-f copy-mode' in config
    assert 'bind-key C-b send-prefix' not in config
    assert 'set -g default-shell /bin/sh' in config
    assert 'set -g status off' in config
    assert 'remain-on-exit off' in config
    assert tmux_backend.backend_command(['list-sessions'], commit=False) is None


def test_tmux_config_appends_xdg_and_project_overrides(
    monkeypatch, tmp_path, tmux_backend
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'xdg'))
    user_config = tmp_path / 'xdg' / 'tq' / 'tmux.conf'
    user_config.parent.mkdir(parents=True)
    user_config.write_text('set -g mouse on\n', encoding='utf-8')
    project_config = tmp_path / '.tq' / 'tmux.conf'
    project_config.parent.mkdir()
    project_config.write_text('set -g status on\n', encoding='utf-8')

    tmux_backend._source_tmux_config()

    sourced = [
        call[0][1]
        for call in tmux_backend.calls
        if call[0][:1] == ('source-file',)
    ]
    assert sourced == [
        str(tmux_backend.tmux_default_config_file),
        str(user_config),
        str(project_config),
    ]
    assert any(
        call[0] == ('set-option', '-g', 'history-limit', '50')
        for call in tmux_backend.calls
    )


def test_tmux_config_uses_parent_project_overrides(
    monkeypatch, tmp_path, tmux_backend
):
    child = tmp_path / 'workspace' / 'nested'
    child.mkdir(parents=True)
    monkeypatch.chdir(child)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'xdg'))
    project_config = tmp_path / 'workspace' / '.tq' / 'tmux.conf'
    project_config.parent.mkdir()
    project_config.write_text('set -g status on\n', encoding='utf-8')

    tmux_backend._source_tmux_config()

    sourced = [
        call[0][1]
        for call in tmux_backend.calls
        if call[0][:1] == ('source-file',)
    ]
    assert sourced == [
        str(tmux_backend.tmux_default_config_file),
        str(project_config),
    ]


def test_tmux_socket_path_uses_xdg_cache_home(monkeypatch, tmp_path):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'cache'))
    backend = TmuxBackend(
        'tmux',
        {
            'backend': 'tmux', 'group': 'g', 'command': 'tmux',
            'socket': 'sock', 'slots': 1,
            'alloc': {}, 'env': {},
        },
    )
    socket_path = tmp_path / 'cache' / 'tq' / 'sock.sock'
    assert backend.socket_path == str(socket_path)
    assert backend._tmux_cmd('ls') == [
        'tmux', '-f', str(backend.tmux_default_config_file),
        '-S', str(socket_path), 'ls'
    ]


def test_tmux_state_dir_uses_xdg_cache_home(monkeypatch, tmp_path):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'cache'))
    backend = TmuxBackend(
        'tmux',
        {
            'backend': 'tmux', 'group': 'g', 'command': 'tmux',
            'socket': 'sock', 'slots': 1, 'alloc': {}, 'env': {},
        },
    )
    assert backend.state_dir == tmp_path / 'cache' / 'tq' / 'sock' / 'g'


def test_tmux_state_dir_falls_back_to_home_cache(monkeypatch, tmp_path):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    monkeypatch.delenv('XDG_CACHE_HOME', raising=False)
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    backend = TmuxBackend(
        'tmux',
        {
            'backend': 'tmux', 'group': 'g', 'command': 'tmux',
            'socket': 'sock', 'slots': 1, 'alloc': {}, 'env': {},
        },
    )
    assert backend.state_dir == (
        tmp_path / 'home' / '.cache' / 'tq' / 'sock' / 'g'
    )


def test_tmux_ignores_configured_state_dir(monkeypatch, tmp_path):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'cache'))
    backend = TmuxBackend(
        'tmux',
        {
            'backend': 'tmux', 'group': 'g', 'command': 'tmux',
            'socket': 'sock', 'slots': 1, 'alloc': {}, 'env': {},
            'state_dir': str(tmp_path / 'ignored'),
        },
    )
    assert 'state_dir' not in backend.config
    assert backend.state_dir == tmp_path / 'cache' / 'tq' / 'sock' / 'g'


def test_tmux_ignores_configured_socket_path(monkeypatch, tmp_path):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'cache'))
    backend = TmuxBackend(
        'tmux',
        {
            'backend': 'tmux', 'group': 'g', 'command': 'tmux',
            'socket': 'sock', 'socket_path': str(tmp_path / 'ignored.sock'),
            'slots': 1, 'alloc': {}, 'env': {},
        },
    )
    assert 'socket_path' not in backend.config
    assert backend.socket_path == str(tmp_path / 'cache' / 'tq' / 'sock.sock')


def test_tmux_add_queues_job_and_ensures_broker(tmux_backend):
    tmux_backend._nvidia_gpus_available = lambda: True
    job_id = tmux_backend.add('echo hi', gpus=1, slots=2, depends_on=[3, 4])
    meta = read_meta(tmux_backend, int(job_id))
    assert meta['status'] == 'queued'
    assert meta['argv'] == ['echo', 'hi']
    assert meta['gpus_required'] == 1
    assert meta['slots_required'] == 2
    assert meta['depends_on'] == [3, 4]
    assert meta['gpu_ids'] == ''
    wrapper = (tmux_backend._job_dir(int(job_id)) / 'run.sh').read_text()
    assert wrapper.startswith('#!/bin/sh\n')
    assert 'set -- echo hi' in wrapper
    assert '    "$@"' in wrapper
    assert 'CUDA_VISIBLE_DEVICES' in wrapper
    assert 'exec "${SHELL:-/bin/sh}"' not in wrapper
    assert 'bash -lc' not in wrapper
    assert 'exit "$exitcode"' in wrapper
    assert 'exec > >(tee' not in wrapper
    assert 'complete_job.py' not in wrapper
    assert "<<'PY'" not in wrapper
    assert '>> "$output_file"' in wrapper
    assert 'while [ "$i" -lt 100 ]; do' in wrapper
    assert meta['start_file'].endswith('/start')
    assert tmux_backend.broker_session in tmux_backend.sessions


def test_tmux_add_captures_enqueue_environment(monkeypatch, tmux_backend):
    monkeypatch.setenv('FOO', 'from-shell')
    monkeypatch.setenv('CONFIG_WINS', 'from-shell')
    monkeypatch.setenv('API_TOKEN', 'secret')
    tmux_backend.env['CONFIG_WINS'] = 'from-config'

    job_id = tmux_backend.add('printenv FOO CONFIG_WINS', gpus=0, slots=1)

    wrapper = (tmux_backend._job_dir(int(job_id)) / 'run.sh').read_text()
    assert 'export FOO=from-shell' in wrapper
    assert 'export CONFIG_WINS=from-shell' in wrapper
    assert 'export CONFIG_WINS=from-config' not in wrapper
    assert 'export API_TOKEN=secret' in wrapper


def test_tmux_add_gpu_job_fails_immediately_without_nvidia(tmux_backend, capsys):
    tmux_backend._nvidia_gpus_available = lambda: False

    job_id = tmux_backend.add('echo gpu', gpus=1, slots=1)

    meta = read_meta(tmux_backend, int(job_id))
    assert meta['status'] == 'failed'
    assert 'nvidia-smi is not available' in (
        tmux_backend._job_dir(int(job_id)) / 'output.log'
    ).read_text(encoding='utf-8')
    assert 'nvidia-smi is not available' in capsys.readouterr().err


def test_tmux_add_cpu_job_does_not_check_nvidia(tmux_backend):
    def fail_if_called():
        raise AssertionError('unexpected GPU availability check')

    tmux_backend._nvidia_gpus_available = fail_if_called

    job_id = tmux_backend.add('echo cpu', gpus=0, slots=1)

    assert read_meta(tmux_backend, int(job_id))['status'] == 'queued'


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


def test_tmux_config_slots_updates_current_broker_config(tmux_backend):
    tmux_backend.sessions.add(tmux_backend.broker_session)
    tmux_backend._broker_version = lambda: TmuxBackend.BROKER_VERSION

    assert tmux_backend.backend_getset('slots', '4') == 4

    killed = [call[0][2] for call in tmux_backend.calls
              if call[0][:2] == ('kill-session', '-t')]
    assert tmux_backend.broker_session not in killed
    broker_commands = [
        call[0][-1] for call in tmux_backend.calls
        if call[0][:3] == ('new-session', '-d', '-s')
        and call[0][3] == tmux_backend.broker_session
    ]
    assert not broker_commands
    config = json.loads(tmux_backend.broker_config_file.read_text())
    assert config['slots'] == 4


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
    assert tmux_backend.attached == [meta['session']]
    assert 'waiting for a slot' in capsys.readouterr().out


def test_tmux_interact_attaches(monkeypatch, tmux_backend):
    monkeypatch.setenv('TMUX', '/tmp/outer-tmux,1,0')
    job_id = int(tmux_backend.add('sleep 1', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    tmux_backend.sessions.add(meta['session'])
    tmux_backend.interact({'id': job_id})
    assert tmux_backend.attached == [meta['session']]
    assert not any(call[0][0] == 'switch-client' for call in tmux_backend.calls)


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


def test_tmux_backend_reset_resets_next_id(tmux_backend):
    job_id = int(tmux_backend.add('echo first', gpus=0, slots=1))
    assert job_id == 1
    tmux_backend.remove({'id': job_id})

    tmux_backend.backend_reset(None)

    next_job_id = int(tmux_backend.add('echo second', gpus=0, slots=1))
    assert next_job_id == 1
    assert tmux_backend.counter_file.read_text(encoding='utf-8') == '2'


def test_tmux_refresh_marks_missing_running_session_interrupted(tmux_backend):
    job_id = int(tmux_backend.add('echo hi', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    meta['status'] = 'running'
    (tmux_backend._job_dir(job_id) / 'meta.json').write_text(json.dumps(meta))
    info = tmux_backend.job_info(ids=[job_id], filters=FilterArgs())[0]
    assert info['status'] == 'interrupted'
    assert info['exitcode'] is None
    meta = read_meta(tmux_backend, job_id)
    assert meta['status'] == 'interrupted'
    assert meta['exitcode'] is None


def test_tmux_refresh_keeps_completed_status_after_session_exits(tmux_backend):
    job_id = int(tmux_backend.add('echo hi', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    meta.update({'status': 'success', 'exitcode': 0})
    (tmux_backend._job_dir(job_id) / 'meta.json').write_text(json.dumps(meta))
    info = tmux_backend.job_info(ids=[job_id], filters=FilterArgs())[0]
    assert info['status'] == 'success'


def test_tmux_refresh_keeps_new_gated_wrapper_running(tmux_backend):
    job_id = int(tmux_backend.add('sleep 10', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    meta.update({
        'status': 'running',
        'start_time': '2024-01-01T00:00:00',
        'pid': 1234,
    })
    (tmux_backend._job_dir(job_id) / 'meta.json').write_text(json.dumps(meta))
    tmux_backend.sessions.add(meta['session'])
    tmux_backend._capture_pane = lambda session, tail: ''
    tmux_backend._pane_current_command = lambda session: 'bash'
    info = tmux_backend.job_info(ids=[job_id], filters=FilterArgs())[0]
    assert info['status'] == 'running'

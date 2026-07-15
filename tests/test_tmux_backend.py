import json
import subprocess
from pathlib import Path

import pytest

from taskq import TOOL_NAME
from taskq.actions.write import RerunAction
from taskq.backends import BACKENDS
from taskq.backends import git_ref as git_ref_utils
from taskq.backends.base import BackendError
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
            'queue': 'test',
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


def git(repo, *args):
    return subprocess.run(
        ['git', '-C', str(repo), *args],
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()


def init_git_repo(path):
    path.mkdir()
    git(path, 'init')
    git(path, 'config', 'user.email', 'taskq@example.test')
    git(path, 'config', 'user.name', 'Taskq Tests')
    git(path, 'config', 'commit.gpgsign', 'false')
    (path / 'script.sh').write_text('echo one\n', encoding='utf-8')
    git(path, 'add', '.')
    git(path, 'commit', '-m', 'one')
    first = git(path, 'rev-parse', 'HEAD')
    (path / 'script.sh').write_text('echo two\n', encoding='utf-8')
    git(path, 'commit', '-am', 'two')
    second = git(path, 'rev-parse', 'HEAD')
    return first, second


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
    user_config = tmp_path / 'xdg' / TOOL_NAME / 'tmux.conf'
    user_config.parent.mkdir(parents=True)
    user_config.write_text('set -g mouse on\n', encoding='utf-8')
    project_config = tmp_path / f'.{TOOL_NAME}' / 'tmux.conf'
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
    project_config = (
        tmp_path / 'workspace' / f'.{TOOL_NAME}' / 'tmux.conf'
    )
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
            'backend': 'tmux', 'queue': 'g', 'command': 'tmux',
            'socket': 'sock', 'slots': 1,
            'alloc': {}, 'env': {},
        },
    )
    socket_path = tmp_path / 'cache' / TOOL_NAME / 'sock.sock'
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
            'backend': 'tmux', 'queue': 'g', 'command': 'tmux',
            'socket': 'sock', 'slots': 1, 'alloc': {}, 'env': {},
        },
    )
    assert backend.state_dir == (
        tmp_path / 'cache' / TOOL_NAME / 'sock' / 'g'
    )


def test_tmux_state_dir_falls_back_to_home_cache(monkeypatch, tmp_path):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    monkeypatch.delenv('XDG_CACHE_HOME', raising=False)
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    backend = TmuxBackend(
        'tmux',
        {
            'backend': 'tmux', 'queue': 'g', 'command': 'tmux',
            'socket': 'sock', 'slots': 1, 'alloc': {}, 'env': {},
        },
    )
    assert backend.state_dir == (
        tmp_path / 'home' / '.cache' / TOOL_NAME / 'sock' / 'g'
    )


def test_tmux_ignores_configured_state_dir(monkeypatch, tmp_path):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'cache'))
    backend = TmuxBackend(
        'tmux',
        {
            'backend': 'tmux', 'queue': 'g', 'command': 'tmux',
            'socket': 'sock', 'slots': 1, 'alloc': {}, 'env': {},
            'state_dir': str(tmp_path / 'ignored'),
        },
    )
    assert 'state_dir' not in backend.config
    assert backend.state_dir == (
        tmp_path / 'cache' / TOOL_NAME / 'sock' / 'g'
    )


def test_tmux_ignores_configured_socket_path(monkeypatch, tmp_path):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'cache'))
    backend = TmuxBackend(
        'tmux',
        {
            'backend': 'tmux', 'queue': 'g', 'command': 'tmux',
            'socket': 'sock', 'socket_path': str(tmp_path / 'ignored.sock'),
            'slots': 1, 'alloc': {}, 'env': {},
        },
    )
    assert 'socket_path' not in backend.config
    assert backend.socket_path == str(
        tmp_path / 'cache' / TOOL_NAME / 'sock.sock'
    )


def test_tmux_add_queues_job_and_ensures_broker(tmux_backend):
    tmux_backend._nvidia_gpus_available = lambda: True
    job_id = tmux_backend.add('echo hi', gpus=1, slots=2, depends_on=[3, 4])
    meta = read_meta(tmux_backend, int(job_id))
    assert meta['status'] == 'queued'
    assert len(meta['submission_id']) == 32
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


def test_tmux_environment_encoding_can_explicitly_unset_server_values():
    encoded = TmuxBackend._encode_env({
        'KEEP_ME': 'value with spaces',
        'DROP_ME': None,
    })

    assert "export KEEP_ME='value with spaces'" in encoded
    assert 'unset DROP_ME' in encoded


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
    meta = read_meta(tmux_backend, int(job_id))
    assert meta['env_file'].endswith('/env.json')
    env = json.loads(
        (tmux_backend._job_dir(int(job_id)) / 'env.json').read_text()
    )
    assert env['FOO'] == 'from-shell'


def test_tmux_add_supports_explicit_cwd_metadata_and_zero_slots(
    tmp_path, tmux_backend
):
    job_id = int(tmux_backend.add(
        'pwd',
        gpus=0,
        slots=0,
        cwd=tmp_path,
        metadata={'campaign': 'speed'},
        internal=True,
        workspace_owner='campaign',
    ))

    meta = read_meta(tmux_backend, job_id)
    assert meta['cwd'] == str(tmp_path.resolve())
    assert meta['slots_required'] == 0
    assert meta['metadata'] == {'campaign': 'speed'}
    assert meta['workspace_owner'] == 'campaign'
    full = tmux_backend.full_info([job_id])[0]
    assert full['slots_required'] == 0
    assert full['metadata'] == {'campaign': 'speed'}
    assert full['workspace_owner'] == 'campaign'


def test_tmux_add_restricts_zero_slots_and_checkout_with_cwd(
    tmp_path, tmux_backend
):
    with pytest.raises(BackendError, match='internal only'):
        tmux_backend.add('true', gpus=0, slots=0)
    with pytest.raises(BackendError, match='cannot be combined'):
        tmux_backend.add('true', gpus=0, slots=1, cwd=tmp_path, git_ref='HEAD')


def test_tmux_registers_and_unregisters_controller(tmp_path, tmux_backend):
    heartbeat = tmp_path / 'heartbeat'
    registered = tmux_backend.register_controller(
        'campaign-1', ['python', '-m', 'controller'], tmp_path, heartbeat,
    )

    path = tmux_backend._controller_file('campaign-1')
    stored = json.loads(path.read_text(encoding='utf-8'))
    assert stored['argv'] == ['python', '-m', 'controller']
    assert stored['cwd'] == str(tmp_path)
    assert stored['heartbeat_file'] == str(heartbeat)
    assert tmux_backend.backend_info()['controllers'][0]['name'] == 'campaign-1'

    tmux_backend.sessions.add(registered['session'])
    tmux_backend.unregister_controller('campaign-1')
    assert not path.exists()
    assert registered['session'] not in tmux_backend.sessions


def test_tmux_rerun_reuses_original_enqueue_environment(
    monkeypatch, tmux_backend
):
    monkeypatch.setenv('HELLO', 'original')
    job_id = int(tmux_backend.add('printenv HELLO', gpus=0, slots=1))
    monkeypatch.setenv('HELLO', 'current')
    action = RerunAction('rerun', {'name': 'rerun'})
    action.backend = tmux_backend

    new_id = int(action.rerun(tmux_backend.full_info([job_id]), True)[0][0])

    wrapper = (tmux_backend._job_dir(new_id) / 'run.sh').read_text()
    env = json.loads((tmux_backend._job_dir(new_id) / 'env.json').read_text())
    assert 'export HELLO=original' in wrapper
    assert 'export HELLO=current' not in wrapper
    assert env['HELLO'] == 'original'
    assert read_meta(tmux_backend, new_id)['submission_id'] != read_meta(
        tmux_backend, job_id)['submission_id']


def test_tmux_add_ref_creates_detached_worktree(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    first, second = init_git_repo(repo)
    monkeypatch.chdir(repo)
    monkeypatch.setenv('HELLO', 'original')

    job_id = int(tmux_backend.add('cat script.sh', gpus=0, slots=1, git_ref='HEAD~1'))

    meta = read_meta(tmux_backend, job_id)
    assert meta['git_ref'] == 'HEAD~1'
    assert meta['git_commit'] == first
    assert meta['git_root'] == str(repo)
    assert meta['source_cwd'] == str(repo)
    assert meta['cwd'] == meta['git_worktree']
    assert (tmux_backend._job_dir(job_id) / 'worktree' / 'script.sh').read_text(
        encoding='utf-8'
    ) == 'echo one\n'
    assert git(repo, 'rev-parse', 'HEAD') == second
    env = json.loads((tmux_backend._job_dir(job_id) / 'env.json').read_text())
    assert env['HELLO'] == 'original'


def test_tmux_add_ref_preserves_relative_cwd(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    subdir = repo / 'subdir'
    subdir.mkdir()
    (subdir / 'file.txt').write_text('hi\n', encoding='utf-8')
    git(repo, 'add', '.')
    git(repo, 'commit', '-m', 'subdir')
    commit = git(repo, 'rev-parse', 'HEAD')
    monkeypatch.chdir(subdir)

    job_id = int(tmux_backend.add('pwd', gpus=0, slots=1, git_ref=commit))

    meta = read_meta(tmux_backend, job_id)
    assert meta['source_cwd'] == str(subdir)
    assert meta['cwd'] == str(tmux_backend._job_dir(job_id) / 'worktree' / 'subdir')


def test_tmux_add_merge_persists_handoff_and_registers_controller(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    _, commit = init_git_repo(repo)
    monkeypatch.chdir(repo)
    registered = []
    monkeypatch.setattr(
        tmux_backend,
        '_register_merge',
        lambda meta: registered.append(meta.copy()),
    )
    spec = {
        'requested': True,
        'target_branch': 'main',
        'git_root': str(repo),
    }

    job_id = int(tmux_backend.add(
        'printf changed > result.txt',
        gpus=0,
        slots=1,
        git_ref='HEAD',
        merge=spec,
    ))

    meta = read_meta(tmux_backend, job_id)
    merge = meta['merge']
    assert merge['requested'] is True
    assert merge['submission_id'] == meta['submission_id']
    assert merge['target_branch'] == 'main'
    assert merge['source_base'] == commit
    assert merge['source_worktree'] == meta['git_worktree']
    assert merge['job_dir'] == str(tmux_backend._job_dir(job_id))
    assert merge['command_result_file'] == meta['command_result_file']
    assert merge['status_file'] == meta['merge_status_file']
    assert merge['result_file'] == meta['merge_result_file']
    assert registered[0]['merge'] == merge
    wrapper = Path(meta['wrapper']).read_text(encoding='utf-8')
    assert 'merge_enabled=1' in wrapper
    assert str(tmux_backend._job_dir(job_id) / 'command-result.json') in wrapper
    assert tmux_backend.full_info([job_id])[0]['merge'] == merge


def test_tmux_resolve_merge_spec_accepts_original_repo_context(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    branch = 'merge-target'
    outside = tmp_path / 'outside'
    outside.mkdir()
    monkeypatch.chdir(outside)

    spec = tmux_backend.resolve_merge_spec(branch, cwd=repo)

    assert spec['repo_root'] == str(repo)
    assert spec['target_branch'] == branch


def test_tmux_merge_lifecycle_fails_closed_when_cancel_is_unconfirmed(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    monkeypatch.chdir(repo)
    monkeypatch.setattr(tmux_backend, '_register_merge', lambda meta: None)
    job_id = int(tmux_backend.add(
        'true', gpus=0, slots=1, git_ref='HEAD',
        merge={
            'requested': True,
            'target_branch': 'main',
            'git_root': str(repo),
        },
    ))
    meta = read_meta(tmux_backend, job_id)
    meta['status'] = 'merging'
    tmux_backend._write_meta(meta)
    tmux_backend.sessions.add(meta['session'])
    monkeypatch.setattr(
        'taskq.merge.workflow.cancel_merge_job',
        lambda current, remove=False: None,
    )

    with pytest.raises(BackendError, match='could not confirm'):
        tmux_backend.kill({'id': job_id})
    with pytest.raises(BackendError, match='could not confirm'):
        tmux_backend.remove({'id': job_id})
    with pytest.raises(BackendError, match='could not confirm'):
        tmux_backend.backend_reset(None)

    assert read_meta(tmux_backend, job_id)['status'] == 'merging'
    assert meta['session'] in tmux_backend.sessions
    assert Path(meta['git_worktree']).exists()


def test_tmux_merge_refresh_kill_preserves_and_remove_cleans_worktree(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    monkeypatch.chdir(repo)
    monkeypatch.setattr(tmux_backend, '_register_merge', lambda meta: None)
    job_id = int(tmux_backend.add(
        'true',
        gpus=0,
        slots=1,
        git_ref='HEAD',
        merge={
            'requested': True,
            'target_branch': 'main',
            'git_root': str(repo),
        },
    ))
    meta = read_meta(tmux_backend, job_id)
    worktree = Path(meta['git_worktree'])
    meta.update({'status': 'running', 'start_time': '2024-01-01T00:00:00'})
    tmux_backend._write_meta(meta)
    tmux_backend.sessions.add(meta['session'])
    Path(meta['command_result_file']).write_text(json.dumps({
        'exitcode': 0,
        'end_time': '2024-01-01T00:00:01',
        'submission_id': meta['submission_id'],
    }), encoding='utf-8')

    info = tmux_backend.full_info([job_id])[0]

    assert info['status'] == 'merging'
    assert info['command_exitcode'] == 0
    assert info.get('exitcode') is None
    assert info['merge']['stage'] == 'queued'
    Path(meta['merge_status_file']).write_text(json.dumps({
        'merge': {
            'stage': 'staged',
            'sequence': 2,
            'submission_id': meta['submission_id'],
        },
    }), encoding='utf-8')
    info = tmux_backend.full_info([job_id])[0]
    assert info['status'] == 'merging'
    assert info['merge']['stage'] == 'staged'

    cancelled = []
    monkeypatch.setattr(
        tmux_backend,
        '_cancel_merge',
        lambda current, remove=False, required=False: (
            cancelled.append((current['id'], remove))
            or {'status': 'cancelled'}
        ),
    )
    tmux_backend.kill({'id': job_id})

    assert cancelled == [(job_id, False)]
    assert read_meta(tmux_backend, job_id)['status'] == 'killed'
    assert worktree.exists()

    tmux_backend.remove({'id': job_id})

    assert cancelled == [(job_id, False), (job_id, True)]
    assert not worktree.exists()


def test_tmux_merge_kill_does_not_overwrite_completed_landing(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    monkeypatch.chdir(repo)
    monkeypatch.setattr(tmux_backend, '_register_merge', lambda meta: None)
    job_id = int(tmux_backend.add(
        'true', gpus=0, slots=1, git_ref='HEAD',
        merge={
            'requested': True,
            'target_branch': 'main',
            'git_root': str(repo),
        },
    ))
    meta = read_meta(tmux_backend, job_id)
    meta['status'] = 'merging'
    tmux_backend._write_meta(meta)
    monkeypatch.setattr(
        tmux_backend,
        '_cancel_merge',
        lambda current, remove=False, required=False: {'status': 'landed'},
    )

    tmux_backend.kill({'id': job_id})

    meta = read_meta(tmux_backend, job_id)
    assert meta['status'] == 'success'
    assert meta['exitcode'] == 0
    assert meta['merge']['stage'] == 'landed'


def test_tmux_merge_refresh_recovers_cancelled_merging_parent(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    monkeypatch.chdir(repo)
    monkeypatch.setattr(tmux_backend, '_register_merge', lambda meta: None)
    job_id = int(tmux_backend.add(
        'true', gpus=0, slots=1, git_ref='HEAD',
        merge={
            'requested': True,
            'target_branch': 'main',
            'git_root': str(repo),
        },
    ))
    meta = read_meta(tmux_backend, job_id)
    meta['status'] = 'merging'
    tmux_backend._write_meta(meta)
    Path(meta['merge_status_file']).write_text(json.dumps({
        'state': 'cancelled',
        'stage': 'cancelled',
        'cancelled': True,
        'submission_id': meta['submission_id'],
    }), encoding='utf-8')

    refreshed = tmux_backend.full_info([job_id])[0]

    assert refreshed['status'] == 'killed'
    assert refreshed['exitcode'] == -1
    assert read_meta(tmux_backend, job_id)['status'] == 'killed'


def test_tmux_add_ref_missing_relative_cwd_cleans_worktree(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    first, _ = init_git_repo(repo)
    subdir = repo / 'subdir'
    subdir.mkdir()
    monkeypatch.chdir(subdir)

    with pytest.raises(Exception, match='does not exist at git commit'):
        tmux_backend.add('pwd', gpus=0, slots=1, git_ref=first)

    assert not list(tmux_backend.jobs_dir.glob('*/meta.json'))
    assert not (tmux_backend._job_dir(1) / 'worktree').exists()
    assert str(tmux_backend._job_dir(1) / 'worktree') not in git(
        repo, 'worktree', 'list', '--porcelain'
    )


def test_tmux_rerun_ref_uses_stored_commit_and_env(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    first, _ = init_git_repo(repo)
    monkeypatch.chdir(repo)
    monkeypatch.setenv('HELLO', 'original')
    job_id = int(tmux_backend.add('cat script.sh', gpus=0, slots=1, git_ref='HEAD~1'))
    monkeypatch.setenv('HELLO', 'current')
    (repo / 'script.sh').write_text('echo three\n', encoding='utf-8')
    git(repo, 'commit', '-am', 'three')
    action = RerunAction('rerun', {'name': 'rerun'})
    action.backend = tmux_backend

    new_id = int(action.rerun(tmux_backend.full_info([job_id]), True)[0][0])

    meta = read_meta(tmux_backend, new_id)
    assert meta['git_commit'] == first
    assert (tmux_backend._job_dir(new_id) / 'worktree' / 'script.sh').read_text(
        encoding='utf-8'
    ) == 'echo one\n'
    env = json.loads((tmux_backend._job_dir(new_id) / 'env.json').read_text())
    assert env['HELLO'] == 'original'


def test_tmux_remove_and_reset_cleanup_git_worktrees(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    monkeypatch.chdir(repo)
    first_id = int(tmux_backend.add('true', gpus=0, slots=1, git_ref='HEAD'))
    second_id = int(tmux_backend.add('true', gpus=0, slots=1, git_ref='HEAD'))
    first_worktree = read_meta(tmux_backend, first_id)['git_worktree']
    second_worktree = read_meta(tmux_backend, second_id)['git_worktree']

    tmux_backend.remove({'id': first_id})
    assert not (tmp_path / first_worktree).exists()
    assert first_worktree not in git(repo, 'worktree', 'list', '--porcelain')

    tmux_backend.backend_reset(None)
    assert not (tmp_path / second_worktree).exists()
    assert second_worktree not in git(repo, 'worktree', 'list', '--porcelain')


def test_campaign_owned_branch_worktree_outlives_job_removal(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    worktree = tmp_path / 'attempt'
    workspace = git_ref_utils.create_branch_worktree(
        repo, 'tq/explore/test/attempt', worktree,
    )
    monkeypatch.chdir(repo)
    job_id = int(tmux_backend.add(
        'true', gpus=0, slots=1, cwd=worktree,
        workspace_owner='campaign',
    ))
    meta = read_meta(tmux_backend, job_id)
    meta.update(workspace)
    tmux_backend._write_meta(meta)

    tmux_backend.remove({'id': job_id})

    assert worktree.is_dir()
    assert workspace['git_branch'] in git(repo, 'branch', '--list')
    git_ref_utils.remove_branch_worktree(
        workspace, delete_branch=True, force_branch=True,
    )
    assert not worktree.exists()
    assert not git(repo, 'branch', '--list', workspace['git_branch'])


def test_merge_owned_staging_worktree_outlives_resolver_job_removal(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    worktree = tmp_path / 'merge-staging'
    workspace = git_ref_utils.create_branch_worktree(
        repo, 'tq/merge/test/staging', worktree,
    )
    monkeypatch.chdir(repo)
    job_id = int(tmux_backend.add(
        'true', gpus=0, slots=1, cwd=worktree,
        workspace_owner='merge',
    ))
    meta = read_meta(tmux_backend, job_id)
    meta.update(workspace)
    meta['workspace_owner'] = 'merge'
    tmux_backend._write_meta(meta)

    tmux_backend.remove({'id': job_id})

    assert worktree.is_dir()
    assert workspace['git_branch'] in git(repo, 'branch', '--list')
    git_ref_utils.remove_branch_worktree(
        workspace, delete_branch=True, force_branch=True,
    )
    assert not worktree.exists()


def test_backend_reset_unregisters_nested_campaign_worktrees(
    monkeypatch, tmp_path, tmux_backend
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    monkeypatch.chdir(repo)
    explore_root = tmux_backend.state_dir / 'explore' / 'campaign'
    mainline = explore_root / 'mainline'
    validation = explore_root / 'validation' / 'candidate'
    git_ref_utils.create_branch_worktree(
        repo, 'tq/explore/reset/mainline', mainline,
    )
    git_ref_utils.create_worktree(
        str(repo), git(repo, 'rev-parse', 'HEAD'), validation,
    )

    tmux_backend.backend_reset(None)

    registered = git(repo, 'worktree', 'list', '--porcelain')
    assert str(mainline) not in registered
    assert str(validation) not in registered
    assert not explore_root.exists()
    assert not git(repo, 'branch', '--list', 'tq/explore/reset/mainline')


def test_tmux_add_gpu_job_fails_immediately_without_nvidia(tmux_backend, capsys):
    tmux_backend._nvidia_gpus_available = lambda: False

    job_id = tmux_backend.add('echo gpu', gpus=1, slots=1)

    meta = read_meta(tmux_backend, int(job_id))
    assert meta['status'] == 'failed'
    assert 'nvidia-smi is not available' in (
        tmux_backend._job_dir(int(job_id)) / 'output.log'
    ).read_text(encoding='utf-8')
    assert 'nvidia-smi is not available' in capsys.readouterr().err


def test_unregistered_gpu_merge_failure_can_be_removed(
    monkeypatch, tmp_path, tmux_backend, capsys
):
    repo = tmp_path / 'repo'
    init_git_repo(repo)
    monkeypatch.chdir(repo)
    tmux_backend._nvidia_gpus_available = lambda: False
    monkeypatch.setattr(
        tmux_backend,
        '_register_merge',
        lambda meta: pytest.fail('failed GPU job must not register a merge'),
    )
    job_id = int(tmux_backend.add(
        'true', gpus=1, slots=1, git_ref='HEAD',
        merge={
            'requested': True,
            'target_branch': 'main',
            'git_root': str(repo),
        },
    ))
    assert read_meta(tmux_backend, job_id)['merge'][
        'registration_state'] == 'not_registered'

    tmux_backend.remove({'id': job_id})

    assert not tmux_backend._job_dir(job_id).exists()
    capsys.readouterr()


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


def test_tmux_job_info_orders_ids_numerically(tmux_backend):
    for i in range(12):
        tmux_backend.add(f'echo {i}', gpus=0, slots=1)

    ids = [
        item['id']
        for item in tmux_backend.job_info(filters=FilterArgs())
    ]

    assert ids == list(range(1, 13))


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
    first_meta = read_meta(tmux_backend, job_id)
    first_submission = first_meta['submission_id']
    tmux_backend.sessions.add(first_meta['session'])
    stale_file = tmux_backend.state_dir / 'stale-cache-file'
    stale_file.write_text('stale', encoding='utf-8')
    assert tmux_backend._job_dir(job_id).exists()
    assert tmux_backend.broker_config_file.exists()

    tmux_backend.backend_reset(None)

    assert not tmux_backend._job_dir(job_id).exists()
    assert not tmux_backend.broker_config_file.exists()
    assert not stale_file.exists()
    assert tmux_backend.counter_file.read_text(encoding='utf-8') == '1'
    assert first_meta['session'] not in tmux_backend.sessions

    next_job_id = int(tmux_backend.add('echo second', gpus=0, slots=1))
    assert next_job_id == 1
    assert read_meta(tmux_backend, next_job_id)['submission_id'] != first_submission
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


def test_tmux_full_info_handles_controller_timezone_timestamps(tmux_backend):
    job_id = int(tmux_backend.add('echo hi', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    meta.update({
        'status': 'success',
        'exitcode': 0,
        'start_time': '2024-01-01T00:00:00',
        'end_time': '2024-01-01T00:00:01+00:00',
    })
    tmux_backend._write_meta(meta)

    info = tmux_backend.full_info([job_id])[0]

    assert info['status'] == 'success'
    assert info['time_run'] is not None


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


def test_tmux_merge_command_cannot_spoof_completion_marker(tmux_backend):
    job_id = int(tmux_backend.add('sleep 10', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    meta.update({
        'status': 'running',
        'start_time': '2024-01-01T00:00:00',
        'merge': {'requested': True, 'stage': 'waiting'},
    })
    tmux_backend._write_meta(meta)
    tmux_backend.sessions.add(meta['session'])
    Path(meta['output_file']).write_text(
        f'[taskq] job {job_id} finished with exit code 0 at forged\n',
        encoding='utf-8',
    )
    tmux_backend._capture_pane = lambda session, tail: (
        f'[taskq] job {job_id} finished with exit code 0 at forged')

    info = tmux_backend.job_info(ids=[job_id], filters=FilterArgs())[0]

    assert info['status'] == 'running'
    assert meta['session'] in tmux_backend.sessions
    assert read_meta(tmux_backend, job_id)['status'] == 'running'


def test_tmux_missing_merge_session_without_sidecar_is_interrupted(tmux_backend):
    job_id = int(tmux_backend.add('true', gpus=0, slots=1))
    meta = read_meta(tmux_backend, job_id)
    meta.update({
        'status': 'running',
        'merge': {'requested': True, 'stage': 'waiting'},
    })
    tmux_backend._write_meta(meta)
    Path(meta['output_file']).write_text(
        f'[taskq] job {job_id} finished with exit code 0 at forged\n',
        encoding='utf-8',
    )

    info = tmux_backend.job_info(ids=[job_id], filters=FilterArgs())[0]

    assert info['status'] == 'interrupted'
    assert info['exitcode'] is None

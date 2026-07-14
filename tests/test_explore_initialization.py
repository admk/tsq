import io
import json
import shlex
import subprocess
import sys
import threading
from pathlib import Path

import pytest
import tomlkit

from taskq.cli import CLI
from taskq.backends.base import BackendError
from taskq.explore.initialization import (
    InitializationInterrupted,
    InitializationStale,
    ProfileInitializationJob,
    ProfileInitializer,
    run_worker,
)
from taskq.explore.profiles import ExploreProfileStore


def git(root, *args):
    return subprocess.run(
        ['git', '-C', str(root), *args],
        capture_output=True, check=True, text=True,
    ).stdout.strip()


def config():
    value = tomlkit.loads(
        Path('taskq/default.toml').read_text(encoding='utf-8'))
    CLI()._hydrate_prompt_assets(value)
    return value


def repo_with_commit(tmp_path):
    root = tmp_path / 'repo'
    root.mkdir()
    git(root, 'init', '-b', 'main')
    git(root, 'config', 'user.name', 'Taskq Tests')
    git(root, 'config', 'user.email', 'taskq@example.test')
    git(root, 'config', 'commit.gpgsign', 'false')
    (root / '.gitignore').write_text('.tq/\n', encoding='utf-8')
    (root / 'app.py').write_text('value = 1\n', encoding='utf-8')
    git(root, 'add', '.gitignore', 'app.py')
    git(root, 'commit', '-m', 'initial')
    return root


class FakeBackend:
    name = 'tmux'

    def __init__(self):
        self.jobs = {}
        self.add_calls = []
        self.interact_calls = []
        self.on_interact = None
        self.add_error = None

    def add(self, command, **kwargs):
        if self.add_error:
            raise self.add_error
        job_id = max(self.jobs, default=0) + 1
        job = {
            'id': job_id,
            'status': 'running',
            'command': command,
            'metadata': kwargs.get('metadata'),
        }
        self.jobs[job_id] = job
        self.add_calls.append((command, kwargs))
        return str(job_id)

    def job_info(self, ids=None, filters=None):
        return [
            {'id': job['id'], 'status': job['status']}
            for job in self.jobs.values()
            if ids is None or job['id'] in ids
        ]

    def full_info(self, ids=None):
        return [
            dict(job) for job in self.jobs.values()
            if ids is None or job['id'] in ids
        ]

    def interact(self, info):
        self.interact_calls.append(info['id'])
        if self.on_interact:
            self.on_interact(self, info['id'])


@pytest.mark.parametrize(
    ('timeout', 'expected_wait'), [(0, None), (30, 30)])
def test_initializer_uses_original_root_and_resolved_environment(
    tmp_path, monkeypatch, timeout, expected_wait,
):
    root = repo_with_commit(tmp_path)
    resolved = config()
    resolved['env'] = {'TOP_LEVEL': 'configured'}
    store = ExploreProfileStore(root, resolved)
    profile = store.create('environment')
    profile.document['explore']['env'] = {
        'VIRTUAL_ENV': '${TASKQ_REPO_ROOT}/.venv',
        'PATH': '${VIRTUAL_ENV}/bin:${PATH}',
    }
    store.save(profile)

    captured = {}

    class Process:
        returncode = 0

        @staticmethod
        def wait(timeout=None):
            captured['timeout'] = timeout
            return 0

    real_popen = subprocess.Popen

    def popen(argv, *args, **kwargs):
        if argv[0] != 'setup-agent':
            return real_popen(argv, *args, **kwargs)
        captured.update({
            'argv': argv,
            'cwd': Path(kwargs['cwd']),
            'env': kwargs['env'],
            'stdio_keys': sorted(
                set(kwargs) & {'stdin', 'stdout', 'stderr'}),
        })
        generated = (
            Path(kwargs['cwd']) / '.tq' / 'explore' / 'environment' /
            'objective.md')
        generated.write_text(
            'Generated campaign objective.\n', encoding='utf-8')
        return Process()

    monkeypatch.setattr(
        'taskq.explore.initialization.subprocess.Popen', popen)

    initialization_config = dict(resolved['explore']['initialization'])
    initialization_config.update({
        'command': ['setup-agent', '{}'], 'timeout': timeout})
    result = ProfileInitializer(
        store, profile, initialization_config,
        objective_prompt='Brief request for the setup agent.',
        stream=io.StringIO()).run()

    assert result is True
    assert captured['env']['TASKQ_REPO_ROOT'] == str(root.resolve())
    assert captured['env']['TASKQ_INIT_WORKTREE'] == str(captured['cwd'])
    assert captured['env']['TOP_LEVEL'] == 'configured'
    assert captured['env']['VIRTUAL_ENV'] == str(root.resolve() / '.venv')
    assert captured['env']['PATH'].startswith(
        str(root.resolve() / '.venv' / 'bin'))
    assert str(root.resolve()) in captured['argv'][-1]
    assert '${TASKQ_REPO_ROOT}/.venv' in captured['argv'][-1]
    assert '`$$` produces a literal dollar' in captured['argv'][-1]
    assert 'Brief request for the setup agent.' in captured['argv'][-1]
    assert captured['timeout'] == expected_wait
    assert captured['stdio_keys'] == []
    assert not captured['cwd'].exists()
    assert store.load('environment').objective == 'Generated campaign objective.'


@pytest.mark.parametrize('other_ref', [False, True])
def test_initializer_falls_back_cleanly_when_repository_has_no_commit(
    tmp_path, monkeypatch, other_ref,
):
    if other_ref:
        root = repo_with_commit(tmp_path)
        git(root, 'checkout', '--orphan', 'empty')
    else:
        root = tmp_path / 'repo'
        root.mkdir()
        git(root, 'init', '-b', 'main')
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('empty')
    real_popen = subprocess.Popen

    def popen(argv, *args, **kwargs):
        if argv[0] == 'git':
            return real_popen(argv, *args, **kwargs)
        pytest.fail('agent must not start without HEAD')

    monkeypatch.setattr(
        'taskq.explore.initialization.subprocess.Popen', popen)
    output = io.StringIO()

    result = ProfileInitializer(
        store, profile, resolved['explore']['initialization'],
        stream=output).run()

    assert result is False
    generation = json.loads(
        store.generation_path(profile).read_text(encoding='utf-8'))
    assert generation['status'] == 'fallback'
    assert generation['attempts'] == 0
    assert 'no commit' in generation['errors'][-1]
    assert 'no commit' in output.getvalue()


def test_initializer_without_command_enters_manual_fallback(tmp_path):
    root = repo_with_commit(tmp_path)
    store = ExploreProfileStore(root, config())
    profile = store.create('manual')

    result = ProfileInitializer(
        store, profile, {}, stream=io.StringIO()).run()

    assert result is False
    assert store.read_generation(profile)['status'] == 'fallback'


def test_initializer_does_not_mask_other_head_failures(tmp_path, monkeypatch):
    import taskq.explore.initialization as initialization

    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('broken-head')
    real_git = initialization.git

    def broken_head(cwd, *args, **kwargs):
        if args == ('rev-parse', 'HEAD'):
            raise BackendError('could not read HEAD')
        return real_git(cwd, *args, **kwargs)

    monkeypatch.setattr(initialization, 'git', broken_head)

    with pytest.raises(BackendError, match='could not read HEAD'):
        ProfileInitializer(
            store, profile, resolved['explore']['initialization'],
            stream=io.StringIO()).run()

    assert not store.generation_path(profile).exists()


def test_initialization_job_queues_attaches_and_persists_request(tmp_path):
    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('queued')
    backend = FakeBackend()

    def finish(current_backend, job_id):
        current_backend.jobs[job_id]['status'] = 'success'
        store.save_generation(profile, status='generated')

    backend.on_interact = finish
    initialization = dict(resolved['explore']['initialization'])
    job = ProfileInitializationJob(
        store, profile, initialization, backend,
        objective_prompt='Investigate startup latency.', stream=io.StringIO())

    assert job.run() is True
    assert len(backend.add_calls) == 1
    command, kwargs = backend.add_calls[0]
    argv = shlex.split(command)
    assert argv[:4] == [
        sys.executable, '-m', 'taskq.explore.initialization', '--worker']
    assert kwargs['gpus'] == 0
    assert kwargs['slots'] == 1
    assert kwargs['cwd'] == str(root.resolve())
    assert kwargs['metadata']['kind'] == job.JOB_KIND
    assert backend.interact_calls == [1]
    generation = json.loads(
        store.generation_path(profile).read_text(encoding='utf-8'))
    assert generation['status'] == 'generated'
    assert generation['backend_job_id'] == '1'
    assert generation['objective_prompt'] == 'Investigate startup latency.'
    assert generation['config']['command'] == initialization['command']
    assert generation['run_token'] == kwargs['metadata']['run_token']


def test_initialization_job_detach_resumes_same_backend_job(tmp_path):
    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('detached')
    backend = FakeBackend()
    output = io.StringIO()

    first = ProfileInitializationJob(
        store, profile, resolved['explore']['initialization'], backend,
        objective_prompt='Objective brief.', stream=output)
    with pytest.raises(InitializationInterrupted) as first_detach:
        first.run()

    second = ProfileInitializationJob(
        store, store.load('detached'),
        resolved['explore']['initialization'], backend,
        objective_prompt='A replacement must not supersede saved state.',
        stream=output)
    with pytest.raises(InitializationInterrupted) as second_detach:
        second.run()

    assert len(backend.add_calls) == 1
    assert backend.interact_calls == [1, 1]
    assert 'continues' in str(first_detach.value)
    assert 'tq interact 1' in str(second_detach.value)
    assert 'paused' not in str(first_detach.value).lower()
    generation = json.loads(
        store.generation_path(profile).read_text(encoding='utf-8'))
    assert generation['objective_prompt'] == 'Objective brief.'


def test_initialization_job_adopts_matching_job_after_save_gap(tmp_path):
    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('adopt')
    backend = FakeBackend()
    token = 'saved-run-token'
    store.save_generation(
        profile, status='queued', run_token=token, backend_job_id=None,
        config={'command': ['agent', '{}']}, objective_prompt='Brief')
    backend.jobs[7] = {
        'id': 7,
        'status': 'running',
        'command': 'worker',
        'metadata': {
            'kind': ProfileInitializationJob.JOB_KIND,
            'repo_root': str(root.resolve()),
            'profile_name': profile.name,
            'run_token': token,
        },
    }

    def finish(current_backend, job_id):
        current_backend.jobs[job_id]['status'] = 'success'
        store.save_generation(profile, status='fallback')

    backend.on_interact = finish

    assert ProfileInitializationJob(
        store, profile, {}, backend, stream=io.StringIO()).run() is True
    assert backend.add_calls == []
    assert backend.interact_calls == [7]
    generation = json.loads(
        store.generation_path(profile).read_text(encoding='utf-8'))
    assert generation['backend_job_id'] == '7'
    assert generation['status'] == 'fallback'


def test_generation_state_lock_merges_parent_and_worker_updates(tmp_path):
    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('state-lock')
    store.save_generation(profile, status='queued', run_token='token')
    started = threading.Event()
    finished = threading.Event()

    def worker_update():
        started.set()
        store.save_generation(
            profile, expected_run_token='token',
            status='running', backend_job_id='9')
        finished.set()

    with store.edit_generation(profile, 'token') as parent_state:
        thread = threading.Thread(target=worker_update)
        thread.start()
        assert started.wait(timeout=1)
        assert not finished.wait(timeout=0.05)
        parent_state['objective_prompt'] = 'Persist both writers.'

    thread.join(timeout=1)
    assert not thread.is_alive()
    generation = store.read_generation(profile)
    assert generation['status'] == 'running'
    assert generation['backend_job_id'] == '9'
    assert generation['objective_prompt'] == 'Persist both writers.'


def test_initialization_job_queue_failure_becomes_fallback(tmp_path):
    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('queue-error')
    backend = FakeBackend()
    backend.add_error = BackendError('broker unavailable')
    output = io.StringIO()

    assert ProfileInitializationJob(
        store, profile, resolved['explore']['initialization'], backend,
        stream=output).run() is True

    generation = json.loads(
        store.generation_path(profile).read_text(encoding='utf-8'))
    assert generation['status'] == 'fallback'
    assert 'broker unavailable' in generation['errors'][-1]
    assert backend.interact_calls == []


def test_initialization_job_interrupted_state_does_not_requeue(tmp_path):
    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('interrupted')
    backend = FakeBackend()
    store.save_generation(
        profile, status='interrupted', run_token='old',
        backend_job_id='4', config={}, objective_prompt='Old brief')

    with pytest.raises(InitializationInterrupted) as interrupted:
        ProfileInitializationJob(
            store, profile, {}, backend, stream=io.StringIO()).run()

    assert backend.add_calls == []
    assert 'was interrupted' in str(interrupted.value)


@pytest.mark.parametrize('job_status', ['failed', 'killed', 'interrupted'])
def test_terminal_job_makes_active_generation_editable(tmp_path, job_status):
    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('retry-{}'.format(job_status))
    backend = FakeBackend()
    backend.jobs[4] = {
        'id': 4,
        'status': job_status,
        'command': 'worker',
        'metadata': {},
    }
    store.save_generation(
        profile, status='running', run_token='old',
        backend_job_id='4', objective_prompt='Revise this brief.')

    generation = ProfileInitializationJob(
        store, profile, resolved['explore']['initialization'], backend,
        stream=io.StringIO()).reconcile_interrupted()

    assert generation['status'] == 'interrupted'
    assert generation['run_token'] == 'old'
    assert job_status in generation['errors'][-1]


def test_successful_generation_is_not_made_editable(tmp_path):
    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('generated')
    backend = FakeBackend()
    backend.jobs[4] = {
        'id': 4,
        'status': 'success',
        'command': 'worker',
        'metadata': {},
    }
    store.save_generation(
        profile, status='generated', run_token='finished',
        backend_job_id='4', objective_prompt='Finished brief.')

    generation = ProfileInitializationJob(
        store, profile, resolved['explore']['initialization'], backend,
        stream=io.StringIO()).reconcile_interrupted()

    assert generation['status'] == 'generated'
    assert generation['run_token'] == 'finished'


def test_worker_loads_persisted_request_and_records_job_id(
    tmp_path, monkeypatch,
):
    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('worker')
    store.save_generation(
        profile, status='queued', run_token='worker-token',
        backend_job_id=None,
        config={'command': ['agent', '{}'], 'prompt': 'prompt'},
        objective_prompt='Persisted brief')
    captured = {}

    def run(self):
        captured['config'] = self.config
        captured['objective_prompt'] = self.objective_prompt
        self.store.save_generation(self.profile, status='generated')
        return True

    monkeypatch.setattr(ProfileInitializer, 'run', run)
    monkeypatch.setenv('TASKQ_JOB_ID', '42')

    assert run_worker(root, profile.name, 'worker-token') == 0
    assert captured == {
        'config': {'command': ['agent', '{}'], 'prompt': 'prompt'},
        'objective_prompt': 'Persisted brief',
    }
    generation = json.loads(
        store.generation_path(profile).read_text(encoding='utf-8'))
    assert generation['backend_job_id'] == '42'
    assert generation['status'] == 'generated'


def test_worker_reports_fallback_as_failed_job(tmp_path, monkeypatch):
    root = repo_with_commit(tmp_path)
    store = ExploreProfileStore(root, config())
    profile = store.create('worker-fallback')
    store.save_generation(
        profile, status='queued', run_token='worker-token',
        config={'command': ['agent', '{}']}, objective_prompt='Brief')

    def run(self):
        self.store.save_generation(self.profile, status='fallback')
        return False

    monkeypatch.setattr(ProfileInitializer, 'run', run)

    assert run_worker(root, profile.name, 'worker-token') == 1
    assert store.read_generation(profile)['status'] == 'fallback'


def test_superseded_initializer_cannot_import_or_finish_old_request(
    tmp_path, monkeypatch,
):
    root = repo_with_commit(tmp_path)
    resolved = config()
    store = ExploreProfileStore(root, resolved)
    profile = store.create('stale-worker')
    store.save_generation(
        profile, status='running', run_token='old-token',
        objective_prompt='Old brief')
    initializer = ProfileInitializer(
        store, profile, resolved['explore']['initialization'],
        objective_prompt='Old brief', run_token='old-token',
        stream=io.StringIO())
    monkeypatch.setattr(
        initializer, '_import',
        lambda *args: pytest.fail('stale worker must not import profile values'))

    store.save_generation(
        profile, status='queued', run_token='new-token',
        objective_prompt='New brief')

    with pytest.raises(InitializationStale):
        initializer._complete_generation(
            object(), tmp_path / 'assets', [], 1, 'old-head')

    generation = store.read_generation(profile)
    assert generation['run_token'] == 'new-token'
    assert generation['status'] == 'queued'
    assert generation['objective_prompt'] == 'New brief'

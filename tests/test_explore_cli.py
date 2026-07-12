import json
import subprocess
from pathlib import Path

import pytest

from taskq.actions.base import INFO
from taskq.backends.base import BackendError
from taskq.cli import CLI
from taskq.explore.state import ExploreState
from taskq.explore.git import ensure_local_exclude
from taskq.explore.workflow import ExploreWorkflow


def git(root, *args):
    return subprocess.run(
        ['git', '-C', str(root), *args],
        capture_output=True, check=True, text=True,
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path):
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


class StubTmuxBackend:
    name = 'tmux'

    def __init__(self, state_dir, config=None):
        self.state_dir = Path(state_dir)
        self.config = config or {'slots': 4, 'queue': 'default'}
        self.registrations = []

    def register_controller(self, *args):
        self.registrations.append(args)


@pytest.fixture
def workflow_env(monkeypatch, tmp_path, repo):
    import taskq.explore.workflow as workflow_module

    reconciled = []

    class StubController:
        def __init__(self, state, backend, campaign_id):
            self.campaign_id = campaign_id

        def reconcile(self):
            reconciled.append(self.campaign_id)

    config = {
        'slots': 4,
        'queue': 'default',
        'explore': {
            'command': ['codex', 'exec', '{}'],
            'parallel': 4,
            'max_adjustments': 3,
            'max_agent_jobs': 32,
            'max_merges': 6,
            'max_wall_time': 28800,
            'max_files': 5,
            'max_lines': 300,
            'protected': ['.tq/**', 'tests/**'],
            'controller_interval': 5,
            'controller_timeout': 30,
        },
    }
    backend = StubTmuxBackend(tmp_path / 'cache', config)
    monkeypatch.setattr(workflow_module, 'ExploreController', StubController)
    monkeypatch.chdir(repo)
    return ExploreWorkflow(backend, config), backend, reconciled


def test_explore_action_is_registered_and_help_lists_subcommands(capsys):
    assert 'explore' in INFO['actions']
    assert INFO['aliases']['explore'] == 'explore'

    with pytest.raises(SystemExit) as error:
        CLI().main(['explore', '--help'])

    assert error.value.code == 0
    output = capsys.readouterr().out
    assert 'run autonomous optimization campaigns' in CLI().parser.format_help()
    assert '{start,status,inspect,pause,resume,stop}' in output
    assert '--cmd' in output


def test_explore_list_output_uses_workflow(monkeypatch, tmp_path, capsys):
    import taskq.actions.explore as explore_action

    class StubWorkflow:
        def __init__(self, backend, config):
            pass

        def list(self):
            return [{
                'id': 'campaign-1',
                'status': 'active',
                'objective': 'reduce latency',
            }]

    rc = tmp_path / 'config.toml'
    rc.write_text('backend = "dummy"\n', encoding='utf-8')
    monkeypatch.setattr(explore_action, 'ExploreWorkflow', StubWorkflow)

    assert CLI().main(['-rc', str(rc), 'explore']) is None
    assert capsys.readouterr().out == 'campaign-1  active  reduce latency\n'

    assert CLI().main(['-rc', str(rc), 'explore', '--json']) is None
    assert json.loads(capsys.readouterr().out) == [{
        'id': 'campaign-1',
        'status': 'active',
        'objective': 'reduce latency',
    }]


def test_explore_rejects_non_tmux_backend(tmp_path, capsys):
    rc = tmp_path / 'config.toml'
    rc.write_text('backend = "dummy"\n', encoding='utf-8')

    code = CLI().main(['-rc', str(rc), 'explore'])

    assert code == 2
    assert 'supported only by the tmux backend' in capsys.readouterr().err


def test_start_requires_clean_repo_including_untracked(
    workflow_env, repo,
):
    workflow, backend, reconciled = workflow_env
    (repo / 'untracked.txt').write_text('dirty\n', encoding='utf-8')

    with pytest.raises(BackendError, match='tracked and untracked'):
        workflow.start('reduce latency')

    assert backend.registrations == []
    assert reconciled == []
    assert not (repo / '.tq' / 'explore' / 'state.sqlite').exists()


def test_runtime_state_is_locally_excluded_from_repo_subdirectory(
    tmp_path, monkeypatch,
):
    root = tmp_path / 'plain-repo'
    root.mkdir()
    git(root, 'init', '-b', 'main')
    git(root, 'config', 'user.name', 'Taskq Tests')
    git(root, 'config', 'user.email', 'taskq@example.test')
    git(root, 'config', 'commit.gpgsign', 'false')
    (root / 'app.py').write_text('value = 1\n', encoding='utf-8')
    subdir = root / 'src'
    subdir.mkdir()
    git(root, 'add', '.')
    git(root, 'commit', '-m', 'initial')
    monkeypatch.chdir(subdir)

    ensure_local_exclude(root)
    state = root / '.tq' / 'explore' / 'state.sqlite'
    state.parent.mkdir(parents=True)
    state.write_text('local state', encoding='utf-8')

    assert git(root, 'status', '--porcelain') == ''
    assert '.tq/explore/' in (
        root / '.git' / 'info' / 'exclude').read_text(encoding='utf-8')


def test_empty_list_and_status_do_not_create_campaign_state(
    workflow_env, repo,
):
    workflow, _, _ = workflow_env
    state_path = repo / '.tq' / 'explore' / 'state.sqlite'

    assert workflow.list() == []
    with pytest.raises(BackendError, match='no exploration campaigns'):
        workflow.status()
    assert not state_path.exists()


def test_start_creates_mainline_state_and_registers_controller(
    workflow_env, repo,
):
    workflow, backend, reconciled = workflow_env

    campaign = workflow.start(
        'reduce latency', name='latency',
        command='agent run --label "safe; still one arg" "{}"',
        checks=['pytest -q'], score='python bench.py', score_direction='min',
        min_improvement=2.5, protect=['fixtures/**'], parallel=2,
    )

    campaign_id = campaign['id']
    state_path = repo / '.tq' / 'explore' / 'state.sqlite'
    assert campaign_id.startswith('latency-')
    assert state_path.is_file()
    assert git(repo, 'status', '--porcelain') == ''
    assert git(repo, 'rev-parse', '--verify', campaign['mainline_ref'])
    assert Path(campaign['config']['mainline_worktree']).is_dir()
    assert Path(campaign['config']['control_cwd']).is_dir()
    assert campaign['config']['command'] == [
        'agent', 'run', '--label', 'safe; still one arg', '{}',
    ]
    assert campaign['config']['checks'] == ['pytest -q']
    assert campaign['config']['protected_paths'] == [
        '.tq/**', 'tests/**', 'fixtures/**',
    ]
    assert campaign['budgets']['parallel'] == 2
    assert campaign['config']['min_improvement'] == 2.5
    assert reconciled == [campaign_id]

    assert len(backend.registrations) == 1
    name, argv, cwd, heartbeat, timeout = backend.registrations[0]
    assert name == campaign_id
    assert argv[1:4] == ['-m', 'taskq.explore.controller', '--state']
    assert argv[-2:] == ['--campaign', campaign_id]
    assert Path(cwd) == repo
    assert Path(heartbeat) == Path(campaign['config']['heartbeat_file'])
    assert timeout == 30

    with ExploreState(state_path) as state:
        assert state.get_campaign(campaign_id) == campaign


@pytest.mark.parametrize('command', ['agent --prompt={}', ''])
def test_start_rejects_unsafe_command_template_as_backend_error(
    workflow_env, command,
):
    workflow, _, _ = workflow_env

    with pytest.raises(BackendError, match='agent command template'):
        workflow.start('reduce latency', command=command)


def test_start_rejects_zero_limits_instead_of_using_defaults(workflow_env):
    workflow, backend, reconciled = workflow_env

    with pytest.raises(BackendError, match='limits must be positive'):
        workflow.start('reduce latency', parallel=0)

    assert backend.registrations == []
    assert reconciled == []


def test_workflow_status_and_inspect_report_attempt_diff(
    workflow_env, repo, tmp_path,
):
    workflow, _, _ = workflow_env
    campaign = workflow.start('reduce latency', name='inspect')
    campaign_id = campaign['id']
    attempt_worktree = tmp_path / 'cache' / 'attempt'
    branch = 'tq/explore/{}/attempt/d001'.format(campaign_id)
    git(repo, 'worktree', 'add', '-b', branch, str(attempt_worktree), 'HEAD')
    (attempt_worktree / 'app.py').write_text('value = 2\n', encoding='utf-8')
    git(attempt_worktree, 'add', 'app.py')
    git(attempt_worktree, 'commit', '-m', 'optimize')
    head = git(attempt_worktree, 'rev-parse', 'HEAD')

    state_path = repo / '.tq' / 'explore' / 'state.sqlite'
    with ExploreState(state_path) as state:
        state.add_direction(
            campaign_id, 'direction-1', 'reduce lookups', 'reduce-lookups')
        state.add_attempt(
            campaign_id, 'attempt-1', 'direction-1', branch,
            attempt_worktree, campaign['target_head'], head=head,
        )

    status = workflow.status(campaign_id)
    inspected = workflow.inspect(campaign_id, 'attempt-1')

    assert status['counts']['attempts'] == 1
    assert status['attempts'][0]['id'] == 'attempt-1'
    assert inspected['campaign']['id'] == campaign_id
    assert len(inspected['attempts']) == 1
    assert '-value = 1' in inspected['attempts'][0]['diff']
    assert '+value = 2' in inspected['attempts'][0]['diff']


def test_status_inspect_and_lifecycle_actions_support_json_and_campaign_ids(
    monkeypatch, tmp_path, capsys,
):
    import taskq.actions.explore as explore_action

    calls = []

    class StubWorkflow:
        def __init__(self, backend, config):
            pass

        def status(self, campaign):
            calls.append(('status', campaign))
            return {
                'campaign': {
                    'id': campaign, 'status': 'active',
                    'objective': 'reduce latency', 'generation': 2,
                },
                'counts': {'attempts': 0},
                'attempts': [], 'merge_requests': [],
                'decisions': [], 'findings': [],
            }

        def inspect(self, campaign, attempt):
            calls.append(('inspect', campaign, attempt))
            return {
                'campaign': {'id': campaign},
                'attempts': [{
                    'id': attempt, 'status': 'active',
                    'worktree': '/tmp/work', 'diff': 'patch',
                }],
            }

        def set_status(self, campaign, status):
            calls.append(('set_status', campaign, status))
            return {'id': campaign, 'status': status}

    rc = tmp_path / 'config.toml'
    rc.write_text('backend = "dummy"\n', encoding='utf-8')
    monkeypatch.setattr(explore_action, 'ExploreWorkflow', StubWorkflow)

    CLI().main([
        '-rc', str(rc), 'explore', 'status', 'campaign-1', '--json',
    ])
    assert json.loads(capsys.readouterr().out)['campaign']['generation'] == 2

    CLI().main([
        '-rc', str(rc), 'explore', 'inspect', 'campaign-1', 'attempt-1',
        '--json',
    ])
    assert json.loads(capsys.readouterr().out)['attempts'][0]['diff'] == 'patch'

    for action, status in (
        ('pause', 'paused'), ('resume', 'active'), ('stop', 'draining'),
    ):
        CLI().main([
            '-rc', str(rc), 'explore', action, 'campaign-1',
        ])
        assert capsys.readouterr().out == 'campaign-1: {}\n'.format(status)

    assert calls == [
        ('status', 'campaign-1'),
        ('inspect', 'campaign-1', 'attempt-1'),
        ('set_status', 'campaign-1', 'paused'),
        ('set_status', 'campaign-1', 'active'),
        ('set_status', 'campaign-1', 'draining'),
    ]


def test_pause_resume_and_stop_persist_and_reconcile(workflow_env):
    workflow, backend, reconciled = workflow_env
    campaign = workflow.start('reduce latency', name='lifecycle')
    campaign_id = campaign['id']

    paused = workflow.set_status(campaign_id, 'paused')
    assert paused['status'] == 'paused'
    assert paused['config']['paused_from'] == 'active'
    assert len(backend.registrations) == 1
    assert reconciled == [campaign_id]

    assert workflow.set_status(campaign_id, 'paused') == paused
    assert len(backend.registrations) == 1

    resumed = workflow.set_status(campaign_id, 'active')
    assert resumed['status'] == 'active'
    assert 'paused_from' not in resumed['config']
    assert len(backend.registrations) == 2
    assert reconciled == [campaign_id, campaign_id]

    stopped = workflow.set_status(campaign_id, 'draining')
    assert stopped['status'] == 'draining'
    assert len(backend.registrations) == 3
    assert reconciled == [campaign_id, campaign_id, campaign_id]

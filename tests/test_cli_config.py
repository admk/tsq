from pathlib import Path

import tomlkit
from taskq import TOOL_NAME
from taskq.cli import CLI


def test_default_config_exposes_tmux_and_ts():
    config = tomlkit.loads(
        Path('taskq/default.toml').read_text(encoding='utf-8'))
    assert config['backend'] == 'tmux'
    assert config['alloc']['gpus'] == 0
    assert 'tmux' in config['backends']
    assert 'ts' in config['backends']
    assert 'state_dir' not in config['backends']['tmux']


def test_default_config_exposes_explore_phases():
    config = tomlkit.loads(
        Path('taskq/default.toml').read_text(encoding='utf-8'))
    explore = config['explore']

    assert set(explore) == {
        'command', 'timeout', 'env', 'response_repair_prompt',
        'planning', 'optimization', 'inspection', 'validation',
        'merge', 'controller', 'initialization',
    }
    assert set(explore['initialization']) == {
        'command', 'timeout', 'prompt', 'repair_prompt'}
    assert explore['initialization']['timeout'] == 0
    assert explore['command'].count('{}') == 1
    assert explore['timeout'] == 30 * 60
    assert explore['env'] == {}
    assert explore['optimization']['parallel'] == 4
    assert explore['optimization']['max_adjustments'] == 3
    assert explore['validation']['gpus'] == 0
    assert explore['merge']['max_accepted_attempts'] == 0
    assert explore['controller']['max_wall_time'] == 8 * 60 * 60
    assert explore['optimization']['max_files'] == 0
    assert explore['optimization']['max_lines'] == 300
    assert explore['optimization']['protected'] == [
        '.tq/**', 'test/**', 'tests/**', 'benchmark/**', 'benchmarks/**',
    ]
    assert explore['controller']['interval'] == 5
    assert explore['controller']['heartbeat_timeout'] == 30
    assert all('timeout' not in explore[phase] for phase in (
        'planning', 'optimization', 'inspection', 'validation', 'merge'))


def test_load_config_uses_packaged_defaults_without_local_rc(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'xdg'))
    args = type('Args', (), {'rc_file': None})()

    config = CLI()._load_config(args)

    assert config['alloc']['gpus'] == 0
    assert config['explore']['planning']['prompt'].startswith('Plan ')
    assert config['explore']['planning']['prompt'] != 'planning.md'
    assert config['explore']['initialization']['prompt'].startswith(
        'Configure ')
    assert '$objective_prompt' in config['explore']['initialization']['prompt']
    assert 'objective.md' in config['explore']['initialization']['prompt']
    assert '$objective_prompt' in config['explore']['initialization'][
        'repair_prompt']
    assert args.rc_file == str(tmp_path / f'.{TOOL_NAME}' / 'config.toml')


def test_load_config_merges_xdg_and_project_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'xdg'))
    user_config = tmp_path / 'xdg' / TOOL_NAME / 'config.toml'
    user_config.parent.mkdir(parents=True)
    user_config.write_text(
        'queue = "user"\nslots = 2\n',
        encoding='utf-8',
    )
    project_config = tmp_path / f'.{TOOL_NAME}' / 'config.toml'
    project_config.parent.mkdir()
    project_config.write_text(
        'queue = "project"\n',
        encoding='utf-8',
    )
    (tmp_path / f'.{TOOL_NAME}.toml').write_text(
        'queue = "legacy"\nslots = 99\n',
        encoding='utf-8',
    )
    args = type('Args', (), {'rc_file': None})()

    config = CLI()._load_config(args)

    assert config['queue'] == 'project'
    assert config['slots'] == 2


def test_load_config_uses_parent_project_config(tmp_path, monkeypatch):
    child = tmp_path / 'workspace' / 'nested'
    child.mkdir(parents=True)
    monkeypatch.chdir(child)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'xdg'))
    project_config = (
        tmp_path / 'workspace' / f'.{TOOL_NAME}' / 'config.toml'
    )
    project_config.parent.mkdir()
    project_config.write_text(
        'queue = "parent"\n',
        encoding='utf-8',
    )
    args = type('Args', (), {'rc_file': None})()

    config = CLI()._load_config(args)

    assert config['queue'] == 'parent'
    assert args.rc_file == str(project_config)


def test_load_config_prefers_nearest_project_config(tmp_path, monkeypatch):
    child = tmp_path / 'workspace' / 'nested'
    child.mkdir(parents=True)
    monkeypatch.chdir(child)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'xdg'))
    parent_config = (
        tmp_path / 'workspace' / f'.{TOOL_NAME}' / 'config.toml'
    )
    parent_config.parent.mkdir()
    parent_config.write_text('queue = "parent"\n', encoding='utf-8')
    child_config = child / f'.{TOOL_NAME}' / 'config.toml'
    child_config.parent.mkdir()
    child_config.write_text('queue = "child"\n', encoding='utf-8')
    args = type('Args', (), {'rc_file': None})()

    config = CLI()._load_config(args)

    assert config['queue'] == 'child'
    assert args.rc_file == str(child_config)


def test_load_config_skips_user_config_when_xdg_config_home_unset(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('XDG_CONFIG_HOME', raising=False)
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    user_config = (
        tmp_path / 'home' / '.config' / TOOL_NAME / 'config.toml'
    )
    user_config.parent.mkdir(parents=True)
    user_config.write_text('queue = "home"\n', encoding='utf-8')
    project_config = tmp_path / f'.{TOOL_NAME}' / 'config.toml'
    project_config.parent.mkdir()
    project_config.write_text('queue = "project"\n', encoding='utf-8')
    args = type('Args', (), {'rc_file': None})()

    config = CLI()._load_config(args)

    assert config['queue'] == 'project'


def test_explicit_rc_merges_packaged_defaults(tmp_path):
    rc = tmp_path / 'tq.toml'
    rc.write_text('queue = "custom"\n', encoding='utf-8')
    args = type('Args', (), {'rc_file': str(rc)})()

    config = CLI()._load_config(args)

    assert config['queue'] == 'custom'
    assert config['alloc']['gpus'] == 0
    assert 'state_dir' not in config['backends']['tmux']


def test_resolve_config_merges_backend_and_queue():
    cli = CLI()
    args = type('Args', (), {'backend': 'tmux', 'queue': 'gpu'})()
    config = tomlkit.parse(
        '''
backend = "ts"
queue = "default"
slots = 1

[alloc]
gpus = 0
slots = 1

[backends.tmux]
command = "tmux"

[queues.gpu]
slots = 4

[queues.gpu.alloc]
gpus = 2
'''
    )
    resolved = cli._resolve_config(args, config)
    assert resolved['backend'] == 'tmux'
    assert resolved['queue'] == 'gpu'
    assert resolved['command'] == 'tmux'
    assert resolved['slots'] == 4
    assert resolved['alloc']['gpus'] == 2

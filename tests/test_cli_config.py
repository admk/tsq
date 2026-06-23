from pathlib import Path

import tomlkit
from taskq.cli import CLI


def test_default_config_exposes_tmux_and_ts():
    config = tomlkit.loads(
        Path('taskq/default.toml').read_text(encoding='utf-8'))
    assert config['backend'] == 'tmux'
    assert config['alloc']['gpus'] == 0
    assert 'tmux' in config['backends']
    assert 'ts' in config['backends']
    assert 'state_dir' not in config['backends']['tmux']


def test_load_config_uses_packaged_defaults_without_local_rc(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'xdg'))
    args = type('Args', (), {'rc_file': None})()

    config = CLI()._load_config(args)

    assert config['alloc']['gpus'] == 0
    assert args.rc_file == str(tmp_path / '.tq' / 'config.toml')


def test_load_config_merges_xdg_and_project_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'xdg'))
    user_config = tmp_path / 'xdg' / 'tq' / 'config.toml'
    user_config.parent.mkdir(parents=True)
    user_config.write_text(
        'group = "user"\nslots = 2\n',
        encoding='utf-8',
    )
    project_config = tmp_path / '.tq' / 'config.toml'
    project_config.parent.mkdir()
    project_config.write_text(
        'group = "project"\n',
        encoding='utf-8',
    )
    (tmp_path / '.tq.toml').write_text(
        'group = "legacy"\nslots = 99\n',
        encoding='utf-8',
    )
    args = type('Args', (), {'rc_file': None})()

    config = CLI()._load_config(args)

    assert config['group'] == 'project'
    assert config['slots'] == 2


def test_load_config_skips_user_config_when_xdg_config_home_unset(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('XDG_CONFIG_HOME', raising=False)
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    user_config = tmp_path / 'home' / '.config' / 'tq' / 'config.toml'
    user_config.parent.mkdir(parents=True)
    user_config.write_text('group = "home"\n', encoding='utf-8')
    project_config = tmp_path / '.tq' / 'config.toml'
    project_config.parent.mkdir()
    project_config.write_text('group = "project"\n', encoding='utf-8')
    args = type('Args', (), {'rc_file': None})()

    config = CLI()._load_config(args)

    assert config['group'] == 'project'


def test_explicit_rc_merges_packaged_defaults(tmp_path):
    rc = tmp_path / 'tq.toml'
    rc.write_text('group = "custom"\n', encoding='utf-8')
    args = type('Args', (), {'rc_file': str(rc)})()

    config = CLI()._load_config(args)

    assert config['group'] == 'custom'
    assert config['alloc']['gpus'] == 0
    assert 'state_dir' not in config['backends']['tmux']


def test_resolve_config_merges_backend_and_group():
    cli = CLI()
    args = type('Args', (), {'backend': 'tmux', 'group': 'gpu'})()
    config = tomlkit.parse(
        '''
backend = "ts"
group = "default"
slots = 1

[alloc]
gpus = 0
slots = 1

[backends.tmux]
command = "tmux"

[groups.gpu]
slots = 4

[groups.gpu.alloc]
gpus = 2
'''
    )
    resolved = cli._resolve_config(args, config)
    assert resolved['backend'] == 'tmux'
    assert resolved['group'] == 'gpu'
    assert resolved['command'] == 'tmux'
    assert resolved['slots'] == 4
    assert resolved['alloc']['gpus'] == 2

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
    assert config['backends']['tmux']['state_dir'] == '~/.cache/taskq'


def test_load_config_uses_packaged_defaults_without_local_rc(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    args = type('Args', (), {'rc_file': None})()

    config = CLI()._load_config(args)

    assert config['alloc']['gpus'] == 0


def test_explicit_rc_merges_packaged_defaults(tmp_path):
    rc = tmp_path / 'tq.toml'
    rc.write_text('group = "custom"\n', encoding='utf-8')
    args = type('Args', (), {'rc_file': str(rc)})()

    config = CLI()._load_config(args)

    assert config['group'] == 'custom'
    assert config['alloc']['gpus'] == 0
    assert config['backends']['tmux']['state_dir'] == '~/.cache/taskq'


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
state_dir = "/tmp/state"

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

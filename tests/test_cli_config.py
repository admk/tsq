from pathlib import Path

import tomlkit
from taskq.cli import CLI


def test_default_config_exposes_tmux_and_ts():
    config = tomlkit.loads(
        Path('taskq/default.toml').read_text(encoding='utf-8'))
    assert config['backend'] == 'tmux'
    assert 'tmux' in config['backends']
    assert 'ts' in config['backends']
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

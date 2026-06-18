import json
import os
import sys
from argparse import Namespace
from pathlib import Path

from taskq.backends.tmux import broker
from taskq.cli import CLI


def run_cli(args, rc_file, capsys):
    code = CLI().main(['-rc', str(rc_file), *args])
    out = capsys.readouterr().out
    return code, out


def install_fake_tmux(tmp_path, monkeypatch):
    state_path = tmp_path / 'fake_tmux_state.json'
    tmux_path = tmp_path / 'tmux'
    tmux_path.write_text(
        f'#!{sys.executable}\n'
        r'''
import json
import os
import sys

state_path = os.environ["TQ_FAKE_TMUX_STATE"]
try:
    with open(state_path, "r", encoding="utf-8") as f:
        state = json.load(f)
except OSError:
    state = {"calls": [], "sessions": {}, "options": {}}

argv = sys.argv[1:]
state["calls"].append(argv)


def save(code=0, stdout=""):
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
        f.write("\n")
    if stdout:
        sys.stdout.write(stdout)
    raise SystemExit(code)


def split_tmux_args(args):
    i = 0
    while i < len(args):
        if args[i] in ("-f", "-L", "-S"):
            i += 2
        else:
            break
    if i >= len(args):
        return "", []
    return args[i], args[i + 1:]


def option_value(args, option):
    try:
        return args[args.index(option) + 1]
    except (ValueError, IndexError):
        return None


cmd, rest = split_tmux_args(argv)
if cmd == "-V":
    save(stdout="tmux 3.4\n")
if cmd == "new-session":
    session = option_value(rest, "-s")
    if session:
        state["sessions"][session] = {"args": rest}
    save()
if cmd == "has-session":
    target = option_value(rest, "-t")
    save(0 if target in state["sessions"] else 1)
if cmd == "kill-session":
    target = option_value(rest, "-t")
    if target:
        state["sessions"].pop(target, None)
    save()
if cmd == "list-sessions":
    save(stdout="\n".join(sorted(state["sessions"])) + "\n")
if cmd == "show-options":
    target = option_value(rest, "-t")
    key = rest[-1] if rest else ""
    save(stdout=state["options"].get(target, {}).get(key, ""))
if cmd == "set-option":
    target = option_value(rest, "-t")
    key = rest[-2] if len(rest) >= 2 else ""
    value = rest[-1] if rest else ""
    state["options"].setdefault(target, {})[key] = value
    save()
if cmd == "display-message":
    save(stdout="4321\n")
if cmd in {"source-file", "pipe-pane", "capture-pane"}:
    save()

save()
''',
        encoding='utf-8',
    )
    tmux_path.chmod(0o755)
    monkeypatch.setenv(
        'PATH',
        str(tmp_path) + os.pathsep + os.environ.get('PATH', ''),
    )
    monkeypatch.setenv('TQ_FAKE_TMUX_STATE', str(state_path))
    return state_path


def read_fake_tmux_calls(state_path):
    return json.loads(state_path.read_text(encoding='utf-8'))['calls']


def write_tmux_rc(rc_file, state_dir, tmux_config):
    def q(value):
        return json.dumps(str(value))

    rc_file.write_text(
        '\n'.join([
            'backend = "tmux"',
            'group = "integration"',
            'slots = 2',
            'socket = "fake-socket"',
            '',
            '[alloc]',
            'gpus = 0',
            'slots = 1',
            '',
            '[env]',
            '',
            '[backends.tmux]',
            'command = "tmux"',
            f'state_dir = {q(state_dir)}',
            f'tmux_config = {q(tmux_config)}',
            'broker_interval = 0.1',
            'history_limit = 1000',
            'gpu_free_perc = 90',
            '',
        ]),
        encoding='utf-8',
    )


def test_tq_add_tmux_uses_shell_agnostic_broker_command(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.setattr('taskq.actions.add.STDIN_TTY', True)
    fake_tmux_state = install_fake_tmux(tmp_path, monkeypatch)
    rc_file = tmp_path / 'tq.toml'
    state_root = tmp_path / 'state'
    tmux_config = tmp_path / 'tmux.conf'
    write_tmux_rc(rc_file, state_root, tmux_config)

    code, out = run_cli(['add', 'sleep', '10'], rc_file, capsys)
    assert code is None
    assert 'Added: 1' in out

    meta_path = next(state_root.glob('**/jobs/1/meta.json'))
    meta = json.loads(meta_path.read_text(encoding='utf-8'))
    assert meta['status'] == 'queued'
    assert meta['command'] == 'sleep 10'
    assert meta['argv'] == ['sleep', '10']

    calls = read_fake_tmux_calls(fake_tmux_state)
    assert all('-L' in call and 'fake-socket' in call for call in calls)
    assert all('-f' in call and str(tmux_config) in call for call in calls)
    broker_call = next(
        call for call in calls
        if 'new-session' in call and meta['session'].rsplit('-', 1)[0] + '-broker' in call
    )
    broker_command = broker_call[-1]
    assert 'taskq.backends.tmux.broker' in broker_command
    assert '--socket fake-socket' in broker_command
    assert f'--config-file {tmux_config}' in broker_command

    broker.tick(Namespace(
        state_dir=str(meta_path.parents[2]),
        prefix=meta['session'].rsplit('-', 1)[0],
        command='tmux',
        config_file=str(tmux_config),
        socket='fake-socket',
        socket_path=None,
        slots=2,
        history_limit=1000,
        interval=0.1,
        gpu_free_perc=90,
        visible_gpus=None,
    ))

    meta = json.loads(meta_path.read_text(encoding='utf-8'))
    assert meta['status'] == 'running'
    assert meta['pid'] == 4321
    assert Path(meta['start_file']).exists()

    calls = read_fake_tmux_calls(fake_tmux_state)
    job_call = next(
        call for call in calls
        if 'new-session' in call and meta['session'] in call
    )
    assert '-e' in job_call
    assert 'TASKQ_GPU_IDS=-1' in job_call
    assert f'TASKQ_WRAPPER={meta["wrapper"]}' in job_call
    assert job_call[-1] == 'exec "$TASKQ_WRAPPER"'
    assert all('TASKQ_GPU_IDS=-1 exec' not in arg for arg in job_call)

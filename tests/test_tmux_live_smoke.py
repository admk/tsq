import json
import re
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from taskq.cli import CLI


def run_cli(args, rc_file, capsys):
    code = CLI().main(['-rc', str(rc_file), *args])
    out = capsys.readouterr().out
    return code, out


def parse_added_id(output):
    match = re.search(r'Added:\s*(\d+)', output)
    assert match, output
    return int(match.group(1))


@pytest.fixture
def live_tmux(tmp_path, monkeypatch):
    tmux = shutil.which('tmux')
    if not tmux:
        pytest.skip('tmux is not installed')
    monkeypatch.setattr('taskq.actions.add.STDIN_TTY', True)
    unique = uuid.uuid4().hex[:8]
    cache_home = Path('/tmp') / f'tq-cache-{unique}'
    monkeypatch.setenv('XDG_CACHE_HOME', str(cache_home))
    socket = f'live-{unique}'
    socket_path = cache_home / 'tq' / f'{socket}.sock'
    state_dir = cache_home / 'tq'
    rc_file = tmp_path / 'tq.toml'
    group = f'live-{unique}'

    def q(value):
        return json.dumps(str(value))

    rc_file.write_text(
        '\n'.join([
            'backend = "tmux"',
            f'group = "{group}"',
            f'socket = "{socket}"',
            'slots = 1',
            '',
            '[alloc]',
            'gpus = 0',
            'slots = 1',
            '',
            '[env]',
            '',
            '[backends.tmux]',
            f'command = {q(tmux)}',
            'broker_interval = 0.05',
            'history_limit = 1000',
            'gpu_free_perc = 90',
            '',
        ]),
        encoding='utf-8',
    )
    try:
        yield {
            'rc_file': rc_file,
            'socket_path': socket_path,
            'state_dir': state_dir,
            'tmux': tmux,
        }
    finally:
        subprocess.run(
            [tmux, '-S', str(socket_path), 'kill-server'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        shutil.rmtree(cache_home, ignore_errors=True)


def meta_path(state_dir, job_id):
    return next(Path(state_dir).glob(f'**/jobs/{job_id}/meta.json'))


def wait_for_meta(path, predicate, timeout=5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = json.loads(Path(path).read_text(encoding='utf-8'))
        if predicate(last):
            return last
        time.sleep(0.05)
    raise AssertionError(f'timed out waiting for job metadata: {last}')


def test_live_tmux_success_and_failed_exit_codes(live_tmux, capsys):
    rc_file = live_tmux['rc_file']

    _, out = run_cli(['add', 'sleep', '0'], rc_file, capsys)
    success_id = parse_added_id(out)
    success = wait_for_meta(
        meta_path(live_tmux['state_dir'], success_id),
        lambda meta: meta['status'] == 'success',
    )
    assert success['exitcode'] == 0

    _, out = run_cli(['add', 'sh', '-c', 'exit 1'], rc_file, capsys)
    failed_id = parse_added_id(out)
    failed = wait_for_meta(
        meta_path(live_tmux['state_dir'], failed_id),
        lambda meta: meta['status'] == 'failed',
    )
    assert failed['exitcode'] == 1


def test_live_tmux_running_job_creates_attachable_session(live_tmux, capsys):
    rc_file = live_tmux['rc_file']
    _, out = run_cli(['add', 'sleep', '10'], rc_file, capsys)
    job_id = parse_added_id(out)
    path = meta_path(live_tmux['state_dir'], job_id)
    meta = wait_for_meta(path, lambda item: item['status'] == 'running')
    result = subprocess.run(
        [
            live_tmux['tmux'],
            '-S',
            str(live_tmux['socket_path']),
            'has-session',
            '-t',
            meta['session'],
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    assert result.returncode == 0

    run_cli(['kill', str(job_id)], rc_file, capsys)
    killed = wait_for_meta(path, lambda item: item['status'] == 'killed')
    assert killed['exitcode'] == -1

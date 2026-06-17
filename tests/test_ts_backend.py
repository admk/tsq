import datetime
import subprocess

import pytest

from taskq.backends import BACKENDS
from taskq.backends.ts import TaskSpoolerBackend
from taskq.common import FilterArgs


@pytest.fixture
def ts_backend(monkeypatch):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)
    monkeypatch.setattr(
        'taskq.backends.base.subprocess.check_output',
        lambda command: b'GPU 0\nGPU 1\n',
    )
    calls = []
    responses = {
        (): 'ID State Output E-Level Times Command\n'
            '1 running /tmp/o1 0 1 echo run\n'
            '2 allocating /tmp/o2 0 1 echo queue\n'
            '3 finished /tmp/o3 0 1 echo ok\n'
            '4 finished /tmp/o4 2 1 echo fail\n'
            '5 finished /tmp/o5 -1 1 echo killed\n',
        ('-S',): '2',
        ('-S', '3'): '',
        ('-V',): 'Task Spooler v1.2.3',
        ('-i', '1'): '\n'.join([
            'Command: echo run',
            'Slots required: 2',
            'GPUs required: 1',
            'GPU IDs: 0',
            'Enqueue time: Mon Jan 01 09:00:00 2024',
            'Start time: Mon Jan 01 09:01:00 2024',
        ]),
        ('-p', '1'): '123',
        ('-o', '1'): '/tmp/out-1',
        ('-c', '1'): 'line1\nline2\nline3',
    }

    def fake_exec(self, *args, commit=True, shell=False, check=True):
        calls.append((tuple(str(a) for a in args), commit, shell, check))
        if not commit:
            return None
        return responses.get(tuple(str(a) for a in args), '')

    monkeypatch.setattr(TaskSpoolerBackend, 'exec', fake_exec)
    backend = TaskSpoolerBackend(
        'ts',
        {
            'backend': 'ts', 'group': 'default', 'command': 'ts',
            'socket': 'sock', 'slots': 2, 'alloc': {'gpus': 1, 'slots': 1},
            'env': {},
        },
    )
    backend.calls = calls
    return backend


def test_ts_registered():
    assert BACKENDS['ts'] is TaskSpoolerBackend


def test_ts_job_info_parses_statuses(ts_backend):
    jobs = ts_backend.job_info()
    assert [(job['id'], job['status'], job['exitcode']) for job in jobs] == [
        (1, 'running', None),
        (2, 'queued', None),
        (3, 'success', 0),
        (4, 'failed', 2),
        (5, 'killed', -1),
    ]

    filtered = ts_backend.job_info(ids=[2], filters=FilterArgs(queued=True))
    assert filtered == [{'id': 2, 'status': 'queued', 'exitcode': None}]


def test_ts_full_info_parses_details(ts_backend):
    info = ts_backend.full_info(ids=[1], filters=FilterArgs(), tqdm_disable=True)[0]
    assert info['command'] == 'echo run'
    assert info['slots_required'] == 2
    assert info['gpus_required'] == 1
    assert info['gpu_ids'] == '0'
    assert info['enqueue_time'] == datetime.datetime(2024, 1, 1, 9, 0, 0)
    assert info['start_time'] == datetime.datetime(2024, 1, 1, 9, 1, 0)
    assert info['output_file'] == '/tmp/out-1'
    assert info['pid'] == 123


def test_ts_add_output_and_write_commands(ts_backend):
    ts_backend.add('echo hello world', gpus=2, slots=3)
    assert ts_backend.calls[-1][0] == ('-G', '2', '-N', '3', 'echo', 'hello', 'world')

    assert ts_backend.output({'id': 1, 'status': 'success'}, 2) == 'line2\nline3'
    ts_backend.output({'id': 1, 'status': 'running'}, 0, shell=True)
    assert ts_backend.calls[-1] == (('-c', '1'), True, True, False)

    ts_backend.kill({'id': 1})
    assert ts_backend.calls[-1][0] == ('-k', '1')

    ts_backend.remove({'id': 1, 'status': 'running'})
    assert (('-w', '1'), True, False, False) in ts_backend.calls
    assert ts_backend.calls[-1][0] == ('-r', '1')


def test_ts_backend_info_getset_and_command(ts_backend):
    assert ts_backend.backend_info()['version'] == 'v1.2.3'
    assert ts_backend.backend_getset('slots') == 2
    ts_backend.backend_getset('slots', 3)
    assert ts_backend.calls[-1][0] == ('-S', '3')
    ts_backend.backend_command(['-V'])
    assert ts_backend.calls[-1] == (('-V',), True, False, False)


def test_ts_init_without_nvidia_smi_uses_one_slot(monkeypatch):
    monkeypatch.setattr('taskq.backends.base.which', lambda command: command)

    def raise_missing(command):
        raise FileNotFoundError

    monkeypatch.setattr(
        'taskq.backends.base.subprocess.check_output', raise_missing)
    monkeypatch.setattr(TaskSpoolerBackend, 'backend_getset', lambda *a, **k: None)
    backend = TaskSpoolerBackend(
        'ts',
        {
            'backend': 'ts', 'group': 'default', 'command': 'ts',
            'socket': 'sock', 'slots': 'auto', 'alloc': {}, 'env': {},
        },
    )
    assert backend.config['slots'] == 1

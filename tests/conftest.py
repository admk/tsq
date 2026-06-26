import datetime
import json
from pathlib import Path

import pytest

from taskq.backends.base import BackendBase


class FakeBackend(BackendBase):
    instances = []
    jobs = []
    added_ids = []

    def __init__(self, name, config):
        config = dict(config)
        config['command'] = ''
        super().__init__(name, config)
        self.calls = []
        FakeBackend.instances.append(self)

    @classmethod
    def reset(cls):
        cls.instances = []
        cls.jobs = [
            {
                'id': 1,
                'status': 'running',
                'exitcode': None,
                'command': 'python train.py',
                'slots_required': 2,
                'gpus_required': 1,
                'gpu_ids': '0',
                'enqueue_time': datetime.datetime(2024, 1, 1, 9, 0, 0),
                'start_time': datetime.datetime(2024, 1, 1, 9, 1, 0),
                'end_time': None,
                'time_run': datetime.timedelta(minutes=5),
                'output_file': 'out-1.log',
                'pid': 123,
            },
            {
                'id': 2,
                'status': 'queued',
                'exitcode': None,
                'command': 'echo queued',
                'slots_required': 1,
                'gpus_required': 0,
                'gpu_ids': '',
                'enqueue_time': datetime.datetime(2024, 1, 1, 10, 0, 0),
                'start_time': None,
                'end_time': None,
                'time_run': None,
                'output_file': 'out-2.log',
            },
            {
                'id': 3,
                'status': 'success',
                'exitcode': 0,
                'command': 'echo done',
                'slots_required': 1,
                'gpus_required': 0,
                'gpu_ids': '',
                'enqueue_time': datetime.datetime(2024, 1, 1, 11, 0, 0),
                'start_time': datetime.datetime(2024, 1, 1, 11, 1, 0),
                'end_time': datetime.datetime(2024, 1, 1, 11, 2, 0),
                'time_run': datetime.timedelta(minutes=1),
                'output_file': 'out-3.log',
            },
            {
                'id': 4,
                'status': 'interrupted',
                'exitcode': None,
                'command': 'sleep 10',
                'slots_required': 1,
                'gpus_required': 0,
                'gpu_ids': '',
                'enqueue_time': datetime.datetime(2024, 1, 1, 12, 0, 0),
                'start_time': datetime.datetime(2024, 1, 1, 12, 1, 0),
                'end_time': None,
                'time_run': datetime.timedelta(minutes=2),
                'output_file': 'out-4.log',
            },
        ]
        cls.added_ids = []

    @staticmethod
    def _filter(jobs, ids=None, filters=None):
        if ids is not None:
            jobs = [job for job in jobs if job['id'] in ids]
        if filters is not None and not filters.all:
            jobs = [
                job for job in jobs
                if getattr(filters, job['status'], False)
            ]
        return jobs

    def backend_info(self):
        return {'name': 'fake', 'command': 'fake'}

    def backend_kill(self, args):
        self.calls.append(('backend_kill', args))

    def backend_command(self, command, commit=True):
        self.calls.append(('backend_command', list(command), commit))
        return 'backend:' + ' '.join(command)

    def backend_getset(self, key, value=None):
        self.calls.append(('backend_getset', key, value))
        if key.endswith('slots'):
            if value is not None:
                self.config['slots'] = value
            return self.config.get('slots')
        return None

    def job_info(self, ids=None, filters=None):
        jobs = self._filter(self.jobs, ids, filters)
        return [
            {
                'id': job['id'],
                'status': job['status'],
                'exitcode': job.get('exitcode'),
            }
            for job in jobs
        ]

    def full_info(
        self, ids=None, filters=None, extra_func=None, tqdm_disable=False
    ):
        jobs = [
            {key: value for key, value in job.items() if value is not None}
            for job in self._filter(self.jobs, ids, filters)
        ]
        for job in jobs:
            if extra_func:
                extra_func(job)
        return jobs

    def output(self, info, tail, shell=False):
        self.calls.append(('output', info['id'], tail, shell))
        return f'output-{info["id"]}'

    def interact(self, info):
        self.calls.append(('interact', info['id']))

    def add(self, command, gpus, slots, depends_on=None, **kwargs):
        call = ('add', command, gpus, slots)
        if depends_on:
            call += (depends_on,)
        if kwargs:
            call += (kwargs,)
        self.calls.append(call)
        new_id = str(100 + len(self.added_ids))
        self.added_ids.append(new_id)
        return new_id

    def kill(self, info, commit=True):
        self.calls.append(('kill', info['id'], commit))

    def remove(self, info, commit=True):
        self.calls.append(('remove', info['id'], commit))


@pytest.fixture(autouse=True)
def reset_fake_backend():
    FakeBackend.reset()


@pytest.fixture
def fake_backend(monkeypatch):
    from taskq.backends import base
    from taskq.cli import CLI

    monkeypatch.setitem(base.BACKENDS, 'fake', FakeBackend)
    choices = list(CLI.base_options[('-b', '--backend')]['choices'])
    if 'fake' not in choices:
        choices.append('fake')
    monkeypatch.setitem(
        CLI.base_options[('-b', '--backend')], 'choices', choices)
    return FakeBackend


@pytest.fixture
def rc_file(tmp_path):
    path = tmp_path / 'tq.toml'
    path.write_text(
        '\n'.join([
            'backend = "fake"',
            'queue = "test"',
            'slots = 2',
            '',
            '[alloc]',
            'gpus = 0',
            'slots = 1',
            '',
            '[backends.fake]',
            'command = ""',
            '',
        ]),
        encoding='utf-8',
    )
    return path


def write_meta(root, job_id, **overrides):
    job_dir = Path(root) / 'jobs' / str(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    wrapper = job_dir / 'run.sh'
    wrapper.write_text('#!/bin/sh\n', encoding='utf-8')
    meta = {
        'id': job_id,
        'command': f'job {job_id}',
        'status': 'queued',
        'exitcode': None,
        'slots_required': 1,
        'gpus_required': 0,
        'gpu_ids': '',
        'enqueue_time': '2024-01-01T00:00:00',
        'start_time': None,
        'end_time': None,
        'output_file': str(job_dir / 'output.log'),
        'wrapper': str(wrapper),
        'session': f'session-{job_id}',
        'pid': None,
        'cwd': str(job_dir),
    }
    meta.update(overrides)
    (job_dir / 'meta.json').write_text(json.dumps(meta), encoding='utf-8')
    return job_dir / 'meta.json'

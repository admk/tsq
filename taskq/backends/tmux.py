import datetime
import fcntl
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

from ..common import STATUSES, dict_simplify, file_tail_lines
from .base import BackendBase, register_backend


@register_backend('tmux')
class TmuxBackend(BackendBase):
    def __init__(self, name, config):
        super().__init__(name, config)
        self.env = dict_simplify(self.env, not_value=True)
        state_dir = self.config.get('state_dir', '~/.local/state/taskq')
        socket = self.config.get('socket', 'taskq')
        group = self.config.get('group', 'default')
        self.state_dir = Path(os.path.expanduser(state_dir)) / socket / group
        self.jobs_dir = self.state_dir / 'jobs'
        self.counter_file = self.state_dir / 'next_id'
        self.prefix = self._sanitize_name(f'taskq-{socket}-{group}')

    @staticmethod
    def _sanitize_name(name):
        name = re.sub(r'[^A-Za-z0-9_.-]+', '-', str(name)).strip('-')
        return name or 'taskq'

    @staticmethod
    def _now():
        return datetime.datetime.now().isoformat()

    @staticmethod
    def _parse_time(value):
        if not value:
            return None
        return datetime.datetime.fromisoformat(value)

    @staticmethod
    def _encode_env(env):
        return '\n'.join(
            f'export {key}={shlex.quote(str(value))}'
            for key, value in env.items()
        )

    @staticmethod
    def _tail_file(path, tail):
        if not path or not os.path.exists(path):
            return ''
        return file_tail_lines(path, tail)

    def _ensure_state(self):
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

    def _next_id(self):
        self._ensure_state()
        lock_file = self.state_dir / 'next_id.lock'
        with open(lock_file, 'w', encoding='utf-8') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                job_id = int(self.counter_file.read_text().strip())
            except (FileNotFoundError, ValueError):
                job_id = 1
            self.counter_file.write_text(str(job_id + 1), encoding='utf-8')
        return job_id

    def _job_dir(self, job_id):
        return self.jobs_dir / str(job_id)

    def _meta_file(self, job_id):
        return self._job_dir(job_id) / 'meta.json'

    def _read_meta(self, job_id):
        with open(self._meta_file(job_id), 'r', encoding='utf-8') as f:
            return json.load(f)

    def _write_meta(self, meta):
        self._ensure_state()
        job_dir = self._job_dir(meta['id'])
        job_dir.mkdir(parents=True, exist_ok=True)
        path = job_dir / 'meta.json'
        tmp = path.with_suffix('.json.tmp')
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(meta, f, indent=2, sort_keys=True)
            f.write('\n')
        os.replace(tmp, path)

    def _all_meta(self):
        if not self.jobs_dir.exists():
            return []
        metas = []
        for path in sorted(self.jobs_dir.glob('*/meta.json')):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    metas.append(json.load(f))
            except (json.JSONDecodeError, OSError):
                continue
        return metas

    def _session_name(self, job_id):
        return f'{self.prefix}-{job_id}'

    def _tmux(self, *args, capture_output=True, check=True):
        cmd = [self._command] + [str(a) for a in args]
        env = dict(os.environ, **self.env)
        if capture_output:
            p = subprocess.run(cmd, capture_output=True, env=env, check=check)
            out = p.stdout.decode('utf-8').strip()
            out += p.stderr.decode('utf-8').strip()
            return out
        subprocess.run(cmd, env=env, check=check)
        return ''

    def _session_exists(self, session):
        result = subprocess.run(
            [self._command, 'has-session', '-t', session],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0

    def _pane_pid(self, session):
        try:
            pid = self._tmux(
                'display-message', '-p', '-t', session, '#{pane_pid}')
            return int(pid) if pid else None
        except (subprocess.CalledProcessError, ValueError):
            return None

    def _capture_pane(self, session, tail):
        if not self._session_exists(session):
            return ''
        args = ['capture-pane', '-p', '-t', session]
        if tail and tail > 0:
            args += ['-S', f'-{tail}']
        else:
            args += ['-S', '-']
        return self._tmux(*args, check=False)

    def _refresh_meta(self, meta):
        if meta.get('status') != 'running':
            return meta
        session = meta.get('session')
        if session and self._session_exists(session):
            pid = self._pane_pid(session)
            if pid:
                meta['pid'] = pid
            return meta
        meta.update({
            'status': 'failed',
            'end_time': self._now(),
        })
        self._write_meta(meta)
        return meta

    def _to_job_info(self, meta):
        meta = self._refresh_meta(meta)
        return {
            'id': int(meta['id']),
            'status': meta.get('status', 'failed'),
            'exitcode': meta.get('exitcode'),
        }

    def _full_from_meta(self, meta):
        meta = self._refresh_meta(meta)
        start_time = self._parse_time(meta.get('start_time'))
        end_time = self._parse_time(meta.get('end_time'))
        if not start_time:
            delta = None
        elif not end_time:
            delta = datetime.datetime.now() - start_time
        else:
            delta = end_time - start_time
        info = {
            'id': int(meta['id']),
            'status': meta.get('status', 'failed'),
            'exitcode': meta.get('exitcode'),
            'command': meta.get('command', ''),
            'slots_required': int(meta.get('slots_required') or 1),
            'gpus_required': int(meta.get('gpus_required') or 0),
            'gpu_ids': meta.get('gpu_ids', ''),
            'enqueue_time': self._parse_time(meta.get('enqueue_time')),
            'start_time': start_time,
            'end_time': end_time,
            'time_run': delta,
            'output_file': meta.get('output_file'),
            'session': meta.get('session'),
            'pid': meta.get('pid'),
            'cwd': meta.get('cwd'),
        }
        return {k: v for k, v in info.items() if v is not None}

    def _filter_info(self, info, ids=None, filters=None):
        if ids is not None:
            info = [i for i in info if i['id'] in ids]
        if filters is not None and not filters.all:
            for status in STATUSES:
                if getattr(filters, status):
                    continue
                info = [i for i in info if i['status'] != status]
        return info

    def backend_getset(self, key, value=None):
        slot_keys = [
            'slots',
            f'backends.{self.name}.slots',
            f'groups.{self.config["group"]}.slots',
        ]
        if key not in slot_keys:
            return None
        if value is not None:
            self.config['slots'] = value
        return self.config.get('slots')

    def backend_info(self):
        info = {
            'name': 'tmux',
            'command': self._command,
            'state_dir': str(self.state_dir),
            'prefix': self.prefix,
        }
        version = self.exec('-V') or ''
        result = re.search(r'tmux\s+(.+)$', version)
        if result:
            info['version'] = result.group(1)
        return info

    def backend_kill(self, args):
        sessions = self._tmux('list-sessions', '-F', '#S', check=False)
        for session in sessions.splitlines():
            if session.startswith(f'{self.prefix}-'):
                self._tmux('kill-session', '-t', session, check=False)

    def job_info(self, ids=None, filters=None):
        info = [self._to_job_info(meta) for meta in self._all_meta()]
        return self._filter_info(info, ids, filters)

    def full_info(
        self, ids=None, filters=None, extra_func=None, tqdm_disable=False
    ):
        info = [self._full_from_meta(meta) for meta in self._all_meta()]
        info = self._filter_info(info, ids, filters)
        for item in info:
            if extra_func:
                extra_func(item)
        return info

    def output(self, info, tail, shell=False):
        meta = self._read_meta(info['id'])
        session = meta.get('session')
        if shell:
            if session and self._session_exists(session):
                command = 'switch-client' if os.environ.get('TMUX') else 'attach-session'
                return self._tmux(
                    command, '-t', session,
                    capture_output=False, check=False)
            out = self._tail_file(meta.get('output_file'), tail)
            if out:
                print(out)
            return ''
        out = ''
        if session:
            out = self._capture_pane(session, tail)
        if not out:
            out = self._tail_file(meta.get('output_file'), tail)
        return out

    def add(self, command, gpus=None, slots=None, commit=True):
        alloc_config = self.config.get('alloc', {})
        gpus = gpus if gpus is not None else alloc_config.get('gpus', 1)
        slots = slots if slots is not None else alloc_config.get('slots', 1)
        if not commit:
            session = self._session_name('<id>')
            print(
                f'{self._command} new-session -d -s {session} '
                f'-c {shlex.quote(os.getcwd())} {shlex.quote(command)}'
            )
            return '<id>'
        job_id = self._next_id()
        session = self._session_name(job_id)
        job_dir = self._job_dir(job_id)
        output_file = job_dir / 'output.log'
        wrapper = job_dir / 'run.sh'
        cwd = os.getcwd()
        meta = {
            'id': job_id,
            'command': command,
            'status': 'running',
            'exitcode': None,
            'slots_required': slots,
            'gpus_required': gpus,
            'gpu_ids': os.environ.get('CUDA_VISIBLE_DEVICES', ''),
            'enqueue_time': self._now(),
            'start_time': self._now(),
            'end_time': None,
            'output_file': str(output_file),
            'session': session,
            'pid': None,
            'cwd': cwd,
        }
        self._write_meta(meta)
        env_exports = self._encode_env(self.env)
        wrapper.write_text(
            '\n'.join([
                '#!/usr/bin/env bash',
                'set +e',
                env_exports,
                f'export TASKQ_JOB_ID={job_id}',
                f'export TASKQ_SESSION={shlex.quote(session)}',
                f'printf "[taskq] job {job_id} started at %s\\n" "$(date)"',
                f'bash -lc {shlex.quote(command)}',
                'exitcode=$?',
                f'{shlex.quote(sys.executable)} - '
                f'{shlex.quote(str(self._meta_file(job_id)))} '
                '"$exitcode" <<\'PY\'',
                'import datetime',
                'import json',
                'import os',
                'import sys',
                'path, code = sys.argv[1], int(sys.argv[2])',
                'with open(path, "r", encoding="utf-8") as f:',
                '    meta = json.load(f)',
                'meta["exitcode"] = code',
                'meta["status"] = "success" if code == 0 else "failed"',
                'meta["end_time"] = datetime.datetime.now().isoformat()',
                'tmp = path + ".tmp"',
                'with open(tmp, "w", encoding="utf-8") as f:',
                '    json.dump(meta, f, indent=2, sort_keys=True)',
                '    f.write("\\n")',
                'os.replace(tmp, path)',
                'PY',
                f'printf "[taskq] job {job_id} finished with exit code %s at %s\\n" "$exitcode" "$(date)"',
                'export TASKQ_EXIT_CODE="$exitcode"',
                'exec "${SHELL:-/bin/sh}"',
                '',
            ]),
            encoding='utf-8',
        )
        wrapper.chmod(0o700)
        self._tmux(
            'new-session', '-d', '-s', session, '-n', f'job-{job_id}',
            '-c', cwd)
        self._tmux(
            'set-option', '-t', session, 'history-limit',
            str(self.config.get('history_limit', 100000)),
            check=False)
        self._tmux(
            'pipe-pane', '-o', '-t', f'{session}:0.0',
            f'cat >> {shlex.quote(str(output_file))}',
            check=False)
        self._tmux(
            'send-keys', '-t', f'{session}:0.0',
            '-l', shlex.quote(str(wrapper)))
        self._tmux('send-keys', '-t', f'{session}:0.0', 'Enter')
        meta['pid'] = self._pane_pid(session)
        self._write_meta(meta)
        return str(job_id)

    def kill(self, info, commit=True):
        meta = self._read_meta(info['id'])
        session = meta.get('session')
        if not commit:
            print(f'{self._command} kill-session -t {session}')
            return
        if session and self._session_exists(session):
            self._tmux('kill-session', '-t', session, check=False)
        meta.update({
            'status': 'killed',
            'exitcode': -1,
            'end_time': self._now(),
        })
        self._write_meta(meta)

    def remove(self, info, commit=True):
        meta = self._read_meta(info['id'])
        session = meta.get('session')
        if not commit:
            print(f'rm -r {self._job_dir(info["id"])}')
            return
        if session and self._session_exists(session):
            self._tmux('kill-session', '-t', session, check=False)
        shutil.rmtree(self._job_dir(info['id']), ignore_errors=True)

import datetime
import fcntl
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

from ... import TOOL_NAME
from ...common import STATUSES, dict_simplify, file_tail_lines
from ...common import project_config_dir, user_cache_dir, user_config_dir
from ..base import BackendBase, register_backend


def render_wrapper(job_id, argv, session, output_file, start_file, env_exports):
    template = Path(__file__).with_name('wrapper.sh').read_text(encoding='utf-8')
    return template.format(
        argv=shlex.join(argv),
        output_dir=shlex.quote(str(output_file.parent)),
        output_file=shlex.quote(str(output_file)),
        start_file=shlex.quote(str(start_file)),
        env_exports=env_exports,
        job_id=job_id,
        session=shlex.quote(session),
    )


@register_backend('tmux')
class TmuxBackend(BackendBase):
    BROKER_VERSION = '7'

    def __init__(self, name, config):
        super().__init__(name, config)
        self.env = dict_simplify(self.env, not_value=True)
        self.socket = self.config.get('socket', 'taskq')
        self.config.pop('socket_path', None)
        cache_root = user_cache_dir()
        self.socket_path = str(
            cache_root / f'{self._sanitize_name(self.socket)}.sock'
        )
        group = self.config.get('group', 'default')
        self.config.pop('state_dir', None)
        state_name = self.socket
        self.state_dir = (
            cache_root
            / self._sanitize_name(state_name)
            / group
        )
        self.jobs_dir = self.state_dir / 'jobs'
        self.counter_file = self.state_dir / 'next_id'
        self.broker_config_file = self.state_dir / 'broker.json'
        self.tmux_default_config_file = Path(__file__).with_name('default.conf')
        self.prefix = self._sanitize_name(f'taskq-{state_name}-{group}')
        self.broker_session = f'{self.prefix}-broker'

    @staticmethod
    def _sanitize_name(name):
        name = re.sub(r'[^A-Za-z0-9_-]+', '-', str(name)).strip('-')
        return name or 'taskq'

    @staticmethod
    def _now():
        return datetime.datetime.now().isoformat()

    @staticmethod
    def _gpu_unavailable_message(gpus_required):
        return (
            '[taskq] GPU allocation requested '
            f'({gpus_required}), but nvidia-smi is not available '
            'or reported no NVIDIA GPUs. '
            'Install NVIDIA GPU tooling or submit the job with -G 0.'
        )

    @staticmethod
    def _nvidia_gpus_available():
        try:
            result = subprocess.run(
                [
                    'nvidia-smi',
                    '--query-gpu=index',
                    '--format=csv,noheader,nounits',
                ],
                capture_output=True,
                check=True,
                text=True,
            )
        except (FileNotFoundError, subprocess.CalledProcessError):
            return False
        return any(line.strip() for line in result.stdout.splitlines())

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
            if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', key)
        )

    @classmethod
    def _capture_job_env(cls, environ, config_env):
        captured = {
            key: value
            for key, value in environ.items()
        }
        return dict(config_env, **captured)

    @staticmethod
    def _tail_file(path, tail):
        if not path or not os.path.exists(path):
            return ''
        return file_tail_lines(path, tail)

    def _tmux_config_files(self):
        files = [self.tmux_default_config_file]
        config_dir = user_config_dir()
        candidates = []
        if config_dir:
            candidates.append(config_dir / 'tmux.conf')
        candidates.append(project_config_dir() / 'tmux.conf')
        for path in candidates:
            if path.exists():
                files.append(path)
        return files

    def _tmux_global_args(self):
        return ['-f', str(self.tmux_default_config_file)] + self._socket_args()

    def _socket_args(self):
        return ['-S', self.socket_path]

    def _tmux_cmd(self, *args):
        return [self._command] + self._tmux_global_args() + [
            str(a) for a in args]

    def _ensure_state(self):
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

    def _write_broker_config(self):
        self.state_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.broker_config_file.with_suffix('.json.tmp')
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({'slots': self.config.get('slots', 1)}, f,
                      indent=2, sort_keys=True)
            f.write('\n')
        os.replace(tmp, self.broker_config_file)

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

    def _env_file(self, job_id):
        return self._job_dir(job_id) / 'env.json'

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

    def _write_env(self, job_id, env):
        self._ensure_state()
        job_dir = self._job_dir(job_id)
        job_dir.mkdir(parents=True, exist_ok=True)
        path = self._env_file(job_id)
        tmp = path.with_suffix('.json.tmp')
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(env, f, indent=2, sort_keys=True)
            f.write('\n')
        os.replace(tmp, path)

    def _read_env_file(self, path):
        if not path:
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def _all_meta(self):
        if not self.jobs_dir.exists():
            return []
        metas = []
        for path in self.jobs_dir.glob('*/meta.json'):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    metas.append(json.load(f))
            except (json.JSONDecodeError, OSError):
                continue
        metas.sort(key=lambda meta: int(meta['id']))
        return metas

    def _session_name(self, job_id):
        return f'{self.prefix}-{job_id}'

    def _tmux(self, *args, capture_output=True, check=True):
        cmd = self._tmux_cmd(*args)
        env = dict(os.environ, **self.env)
        if capture_output:
            p = subprocess.run(cmd, capture_output=True, env=env, check=check)
            out = p.stdout.decode('utf-8').strip()
            out += p.stderr.decode('utf-8').strip()
            return out
        subprocess.run(cmd, env=env, check=check)
        return ''

    def _attach_tmux(self, session):
        env = dict(os.environ, **self.env)
        env.pop('TMUX', None)
        subprocess.run(
            self._tmux_cmd('attach-session', '-t', session),
            env=env,
            check=False,
        )
        return ''

    def _session_exists(self, session):
        result = subprocess.run(
            self._tmux_cmd('has-session', '-t', session),
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

    def _pane_current_command(self, session):
        try:
            return self._tmux(
                'display-message', '-p', '-t', session,
                '#{pane_current_command}',
            )
        except subprocess.CalledProcessError:
            return ''

    def _capture_pane(self, session, tail):
        if not self._session_exists(session):
            return ''
        args = ['capture-pane', '-p', '-t', session]
        if tail and tail > 0:
            args += ['-S', f'-{tail}']
        else:
            args += ['-S', '-']
        return self._tmux(*args, check=False)

    def _broker_command(self):
        command = [
            sys.executable,
            '-m',
            'taskq.backends.tmux.broker',
            '--state-dir',
            str(self.state_dir),
            '--prefix',
            self.prefix,
            '--command',
            self._command,
            '--slots',
            str(self.config.get('slots', 1)),
            '--history-limit',
            str(self.config.get('history_limit', 100000)),
            '--interval',
            str(self.config.get('broker_interval', 1)),
            '--gpu-free-perc',
            str(self.config.get('gpu_free_perc', 90)),
        ]
        visible_gpus = self.config.get(
            'visible_gpus',
            self.env.get('TS_VISIBLE_DEVICES') or os.environ.get('TS_VISIBLE_DEVICES'),
        )
        if visible_gpus:
            command += ['--visible-gpus', str(visible_gpus)]
        command += ['--socket-path', self.socket_path]
        return shlex.join(command)

    def _source_tmux_config(self):
        for path in self._tmux_config_files():
            self._tmux('source-file', str(path), check=False)
        self._tmux(
            'set-option', '-g', 'history-limit',
            str(self.config.get('history_limit', 100000)),
            check=False,
        )

    def _broker_version(self):
        return self._tmux(
            'show-options', '-qv', '-t', self.broker_session,
            '@taskq_broker_version',
            check=False,
        )

    def _ensure_broker(self, commit=True):
        broker_command = self._broker_command()
        if not commit:
            print(
                ' '.join(self._tmux_cmd(
                    'new-session', '-d', '-s', self.broker_session,
                    '-n', 'broker', broker_command))
            )
            return
        self._ensure_state()
        self._write_broker_config()
        if self._session_exists(self.broker_session):
            self._source_tmux_config()
            if self._broker_version() == self.BROKER_VERSION:
                return
            self._tmux('kill-session', '-t', self.broker_session, check=False)
        self._tmux(
            'new-session', '-d', '-s', self.broker_session,
            '-n', 'broker', '-c', os.getcwd(), broker_command)
        self._source_tmux_config()
        self._tmux(
            'set-option', '-t', self.broker_session,
            '@taskq_broker_version', self.BROKER_VERSION,
            check=False,
        )

    def _wait_for_session(self, job_id):
        while True:
            meta = self._read_meta(job_id)
            session = meta.get('session')
            if session and self._session_exists(session):
                return session
            if meta.get('status') not in ['queued', 'running']:
                return None
            time.sleep(float(self.config.get('broker_interval', 1)))

    def _refresh_meta(self, meta):
        if meta.get('status') != 'running':
            return meta
        session = meta.get('session')
        if session and self._session_exists(session):
            pane_text = self._capture_pane(session, 200)
            exitcode = self._finished_exitcode(
                meta, pane_text)
            if exitcode is not None:
                meta.update({
                    'status': 'success' if exitcode == 0 else 'failed',
                    'exitcode': exitcode,
                    'end_time': meta.get('end_time') or self._now(),
                })
                self._write_meta(meta)
                self._tmux('kill-session', '-t', session, check=False)
                return meta
            pid = self._pane_pid(session)
            if pid:
                meta['pid'] = pid
            return meta
        try:
            meta = self._read_meta(meta['id'])
        except FileNotFoundError:
            return meta
        if meta.get('status') != 'running':
            return meta
        exitcode = self._finished_exitcode(meta)
        if exitcode is not None:
            meta.update({
                'status': 'success' if exitcode == 0 else 'failed',
                'exitcode': exitcode,
                'end_time': meta.get('end_time') or self._now(),
            })
            self._write_meta(meta)
            return meta
        meta.update({
            'status': 'interrupted',
            'exitcode': None,
            'end_time': self._now(),
        })
        self._write_meta(meta)
        return meta

    @staticmethod
    def _finished_exitcode(meta, pane_text=None):
        output_file = meta.get('output_file')
        marker = (
            f'[taskq] job {meta["id"]} finished with exit code ')
        chunks = []
        if output_file:
            try:
                with open(output_file, 'rb') as f:
                    f.seek(0, os.SEEK_END)
                    size = f.tell()
                    f.seek(max(0, size - 65536))
                    chunks.append(f.read().decode('utf-8', errors='replace'))
            except OSError:
                pass
        if pane_text:
            chunks.append(pane_text)
        for data in chunks:
            for line in reversed(data.splitlines()):
                if marker not in line:
                    continue
                value = line.split(marker, 1)[1].split(maxsplit=1)[0]
                try:
                    return int(value)
                except ValueError:
                    return None
        return None

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
            'depends_on': meta.get('depends_on', []),
            'enqueue_time': self._parse_time(meta.get('enqueue_time')),
            'start_time': start_time,
            'end_time': end_time,
            'time_run': delta,
            'output_file': meta.get('output_file'),
            'env_file': meta.get('env_file'),
            'session': meta.get('session'),
            'pid': meta.get('pid'),
            'cwd': meta.get('cwd'),
        }
        return {k: v for k, v in info.items() if v is not None}

    def rerun_env(self, info):
        return self._read_env_file(info.get('env_file'))

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
            if value == 'null':
                return self.config.get('slots')
            old_slots = self.config.get('slots')
            new_slots = self._resolve_slots(value)
            self.config['slots'] = new_slots
            if old_slots != new_slots and self._session_exists(self.broker_session):
                self._write_broker_config()
                if self._broker_version() != self.BROKER_VERSION:
                    self._ensure_broker()
        return self.config.get('slots')

    def backend_command(self, command, commit=True):
        if not commit:
            print(' '.join(self._tmux_cmd(*command)))
            return None
        return self._tmux(*command, check=False)

    def backend_info(self):
        info = {
            'name': 'tmux',
            'command': self._command,
            'socket': self.socket,
            'tmux_socket_path': self.socket_path,
            'tmux_default_config': str(self.tmux_default_config_file),
            'tmux_config_overrides': [
                str(path) for path in self._tmux_config_files()[1:]
            ],
            'state_dir': str(self.state_dir),
            'prefix': self.prefix,
            'broker_session': self.broker_session,
            'broker_running': self._session_exists(self.broker_session),
        }
        version = self._tmux('-V', check=False) or ''
        result = re.search(r'tmux\s+(.+)$', version)
        if result:
            info['version'] = result.group(1)
        return info

    def backend_kill(self, args):
        sessions = self._tmux('list-sessions', '-F', '#S', check=False)
        for session in sessions.splitlines():
            if session.startswith(f'{self.prefix}-'):
                self._tmux('kill-session', '-t', session, check=False)

    def backend_reset(self, args):
        shutil.rmtree(self.state_dir, ignore_errors=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        lock_file = self.state_dir / 'next_id.lock'
        with open(lock_file, 'w', encoding='utf-8') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self.counter_file.write_text('1', encoding='utf-8')

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
            if meta.get('status') == 'queued':
                self._ensure_broker()
                print(f'Job {info["id"]} is queued; waiting for a slot...')
                session = self._wait_for_session(info['id'])
            if session and self._session_exists(session):
                return self._attach_tmux(session)
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

    def add(self, command, gpus=None, slots=None, depends_on=None, env=None):
        alloc_config = self.config.get('alloc', {})
        gpus = gpus if gpus is not None else alloc_config.get('gpus', 0)
        slots = slots if slots is not None else alloc_config.get('slots', 1)
        depends_on = [int(job_id) for job_id in depends_on or []]
        job_id = self._next_id()
        session = self._session_name(job_id)
        job_dir = self._job_dir(job_id)
        output_file = job_dir / 'output.log'
        env_file = self._env_file(job_id)
        wrapper = job_dir / 'run.sh'
        start_file = job_dir / 'start'
        cwd = os.getcwd()
        argv = shlex.split(command)
        job_env = (
            self._capture_job_env(os.environ, self.env)
            if env is None else dict(env)
        )
        meta = {
            'id': job_id,
            'command': command,
            'argv': argv,
            'status': 'queued',
            'exitcode': None,
            'slots_required': slots,
            'gpus_required': gpus,
            'gpu_ids': '',
            'depends_on': depends_on,
            'enqueue_time': self._now(),
            'start_time': None,
            'end_time': None,
            'output_file': str(output_file),
            'env_file': str(env_file),
            'wrapper': str(wrapper),
            'start_file': str(start_file),
            'session': session,
            'pid': None,
            'cwd': cwd,
        }
        self._write_env(job_id, job_env)
        self._write_meta(meta)
        if int(gpus or 0) > 0 and not self._nvidia_gpus_available():
            message = self._gpu_unavailable_message(gpus)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'a', encoding='utf-8') as f:
                f.write(message + '\n')
            meta.update({
                'status': 'failed',
                'end_time': self._now(),
            })
            self._write_meta(meta)
            print(message, file=sys.stderr)
            return str(job_id)
        env_exports = self._encode_env(job_env)
        wrapper.write_text(
            render_wrapper(
                job_id=job_id,
                argv=argv,
                session=session,
                output_file=output_file,
                start_file=start_file,
                env_exports=env_exports,
            ),
            encoding='utf-8',
        )
        wrapper.chmod(0o700)
        self._ensure_broker()
        return str(job_id)

    def kill(self, info, commit=True):
        meta = self._read_meta(info['id'])
        session = meta.get('session')
        if not commit:
            print(f'{self._command} {" ".join(self._socket_args())} kill-session -t {session}')
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

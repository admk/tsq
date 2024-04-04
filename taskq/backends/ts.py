import re
import shlex
import datetime

from ..common import tqdm, STATUSES, file_tail_lines, tail_lines, dict_simplify
from .base import register_backend, BackendBase


@register_backend('ts')
class TaskSpoolerBackend(BackendBase):
    def __init__(self, name, config):
        super().__init__(name, config)
        socket = self.config.get('socket', 'default')
        slots = self.config.get('slots')
        self.env.setdefault('TS_SLOTS', str(slots))
        self.env.setdefault('TS_SOCKET', f'/tmp/ts-{socket}.sock')
        self.env = dict_simplify(self.env, not_value=True)
        self.backend_getset('slots', slots)

    def backend_getset(self, key, value=None):
        slot_keys = [
            'slots',
            f'backends.{self.name}.slots',
            f'groups.{self.config["group"]}.slots'
        ]
        if key in slot_keys:
            if value is None:
                return int(self.exec('-S') or -1)
            return self.exec('-S', value)

    def backend_info(self):
        info = {
            'name': 'Task Spooler',
            'command': self._command,
        }
        version = self.exec('-V') or ''
        result = re.search(r'(v[\.\d]+)', version)
        if result:
            info['version'] = result.group(1)
        return info

    def backend_kill(self, args):
        self.exec('-K')

    def job_info(self, ids=None, filters=None):
        info = []
        tsout = self.exec()
        if not tsout:
            return info
        for l in tsout.splitlines()[1:]:
            l = l.strip().split()
            if l[1] == 'finished':
                job_id, status, _, exitcode, *_ = l
                exitcode = int(exitcode)
                if exitcode == 0:
                    status = 'success'
                elif exitcode < 0:
                    status = 'killed'
                else:
                    status = 'failed'
            else:
                job_id, status, *_ = l
                exitcode = None
            if status == 'allocating':
                status = 'queued'
            info.append({
                'id': int(job_id),
                'status': status,
                'exitcode': exitcode
            })
        if filters is not None and not filters.all:
            for a in STATUSES:
                if getattr(filters, a):
                    continue
                info = [i for i in info if i['status'] != a]
        if ids is not None:
            info = [i for i in info if i['id'] in ids]
        return info

    @classmethod
    def get_line(cls, ji, key):
        for l in ji.splitlines():
            if key in l:
                return l.replace(key, '').strip()
        return None

    @classmethod
    def get_time(cls, ji, key):
        time = cls.get_line(ji, key)
        if not time:
            return None
        return datetime.datetime.strptime(time, '%a %b %d %H:%M:%S %Y')

    def full_info(
        self, ids=None, filters=None, extra_func=None, tqdm_disable=False
    ):
        info = self.job_info(ids, filters)
        for i in tqdm(info, disable=tqdm_disable, desc='info'):
            ji = self.exec('-i', i['id'])
            start_time = self.get_time(ji, 'Start time: ')
            end_time = self.get_time(ji, 'End time: ')
            if not start_time:
                delta = None
            elif not end_time:
                delta = datetime.datetime.now() - start_time
            else:
                delta = end_time - start_time
            try:
                pid = int(self.exec('-p', i['id'], check=False) or -1)
            except ValueError:
                pid = None
            new_info = {
                'command': self.get_line(ji, 'Command: '),
                'slots_required':
                    int(self.get_line(ji, 'Slots required: ') or 1),
                'gpus_required':
                    int(self.get_line(ji, 'GPUs required: ') or 0),
                'gpu_ids': self.get_line(ji, 'GPU IDs: '),
                'enqueue_time': self.get_time(ji, 'Enqueue time: '),
                'start_time': start_time,
                'end_time': end_time,
                'time_run': delta,
                'output_file': self.exec('-o', i['id'], check=False),
                'pid': pid,
            }
            i.update({k: v for k, v in new_info.items() if v is not None})
            if extra_func:
                extra_func(i)
        return info

    def output(self, info, tail, shell=False):
        if shell:
            self.exec('-c', info['id'], check=False, shell=True)
            return ''
        if info['status'] in ['success', 'failed', 'killed']:
            return tail_lines(self.exec('-c', info['id'], check=False), tail)
        f = info.get('output_file') or self.exec('-o', info['id'], check=False)
        return file_tail_lines(f, tail) if f else ''

    def add(self, command, gpus=None, slots=None, commit=True):
        torun = []
        alloc_config = self.config.get('alloc', {})
        gpus = gpus if gpus is not None else alloc_config.get('gpus', 1)
        slots = slots if slots is not None else alloc_config.get('slots', 1)
        if gpus > 0:
            torun += ['-G', gpus]
        torun += ['-N', slots]
        command = command.replace('\n', '\\n')
        torun += shlex.split(command)
        return self.exec(*torun, commit=commit)

    def kill(self, info, commit=True):
        self.exec('-k', info['id'], commit=commit)

    def remove(self, info, commit=True):
        if info['status'] == 'running':
            self.kill(info, commit=commit)
            self.exec('-w', info['id'], commit=commit, check=False)
        return self.exec('-r', info['id'], commit=commit)

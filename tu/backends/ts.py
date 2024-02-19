import shlex
import datetime
import subprocess

from ..common import tqdm, STATUSES, file_tail_lines, tail_lines
from .base import register_backend, BackendBase


@register_backend('ts')
class TaskSpooler(BackendBase):
    def _ts(self, *args, commit=True, interactive=False):
        cmd = ['ts'] + [str(a) for a in args]
        if not commit:
            print(' '.join(cmd))
            return None
        if not interactive:
            p = subprocess.run(cmd, capture_output=True)
            return p.stdout.decode('utf-8').strip()
        subprocess.run(cmd, shell=True)
        return None

    def job_info(self, ids, filters):
        info = []
        for l in self._ts().splitlines()[1:]:
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
            info.append({
                'id': int(job_id),
                'status': status,
                'exitcode': exitcode
            })
        if not filters.all:
            for a in STATUSES:
                if getattr(filters, a):
                    continue
                info = [i for i in info if i['status'] != a]
        if ids:
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

    def full_info(self, ids, filters, extra_func=None, tqdm_disable=False):
        info = self.job_info(ids, filters)
        for i in tqdm(info, disable=tqdm_disable):
            ji = self._ts('-i', i['id'])
            start_time = self.get_time(ji, 'Start time: ')
            end_time = self.get_time(ji, 'End time: ')
            if not start_time:
                delta = None
            elif not end_time:
                delta = datetime.datetime.now() - start_time
            else:
                delta = end_time - start_time
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
                'output_file': self._ts('-o', i['id']),
            }
            i.update({k: v for k, v in new_info.items() if v is not None})
            if extra_func:
                extra_func(i)
        return info

    def output(self, info, tail):
        if info['status'] in ['success', 'failed', 'killed']:
            return tail_lines(self._ts('-c', info['id']), tail)
        f = info.get('output_file') or self._ts('-o', info['id'])
        return file_tail_lines(f, tail) if f else ''

    def add(self, command, gpus, slots, commit=True):
        torun = []
        if gpus:
            torun += ['-G', gpus]
        if slots:
            torun += ['-N', slots]
        command = command.replace('\n', '\\n')
        torun += shlex.split(command)
        return self._ts(*torun, commit=commit)

    def kill(self, info, commit=True):
        self._ts('-k', info['id'], commit=commit)

    def remove(self, info, commit=True):
        if info['status'] == 'running':
            self.kill(info, commit=commit)
        return self._ts('-r', info['id'], commit=commit)

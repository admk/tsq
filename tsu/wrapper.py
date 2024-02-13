import sys
import shlex
import datetime
import subprocess

from tqdm import tqdm as tqdm_


def tqdm(*args, commit=True, **kwargs):
    disable = not commit or not sys.stdout.isatty()
    return tqdm_(*args, **kwargs, disable=disable)


STATUSES = ['running', 'allocating', 'success', 'failed', 'killed']


def _ts(*args, commit=True):
    cmd = ['ts'] + [str(a) for a in args]
    if not commit:
        print(' '.join(cmd))
        return
    p = subprocess.run(cmd, capture_output=True)
    return p.stdout.decode('utf-8').strip()


def job_info(ids, filters):
    info = {}
    for l in _ts().splitlines()[1:]:
        l = l.strip().split()
        if l[1] == 'finished':
            id, status, _, exitcode, gpus, *_ = l
            exitcode = int(exitcode)
            if exitcode == 0:
                status = 'success'
            elif exitcode < 0:
                status = 'killed'
            else:
                status = 'failed'
        else:
            id, status, _, gpus, *_ = l
            exitcode = None
        info[int(id)] = {
            'status': status,
            'gpus': gpus,
            'exitcode': exitcode
        }
    if not filters.all:
        for a in STATUSES:
            if not getattr(filters, a):
                info = {k: v for k, v in info.items() if v['status'] != a}
    if ids:
        info = {k: v for k, v in info.items() if k in ids}
    return {k: info[k] for k in sorted(info)}


def full_info(ids, filters):
    def get_line(ji, key):
        for l in ji.splitlines():
            if key in l:
                return l.replace(key, '').strip()
        return None

    def get_time(ji, key):
        time = get_line(ji, key)
        if not time:
            return None
        return datetime.datetime.strptime(time, '%a %b %d %H:%M:%S %Y')

    def get_time_run(ji, key):
        time = get_line(ji, key)
        if not time:
            return None
        return time

    info = job_info(ids, filters)
    for i in info:
        ji = _ts('-i', i)
        new_info = {
            'command': get_line(ji, 'Command: '),
            'slots_required': int(get_line(ji, 'Slots required: ') or 1),
            'gpus_required': int(get_line(ji, 'GPUs required: ') or 0),
            'gpu_ids': get_line(ji, 'GPU IDs: '),
            'enqueue_time': get_time(ji, 'Enqueue time: '),
            'start_time': get_time(ji, 'Start time: '),
            'end_time': get_time(ji, 'End time: '),
            'time_run':
                get_time_run(ji, 'Time running: ') or
                get_time_run(ji, 'Time run: '),
        }
        info[i].update({k: v for k, v in new_info.items() if v is not None})
    return info


def add(command, gpus, slots, commit=True):
    torun = []
    if gpus:
        torun += ['-G', gpus]
    if slots:
        torun += ['-N', slots]
    torun += shlex.split(command)
    return _ts(*torun, commit=commit)


def remove(id, commit=True):
    return _ts('-r', id, commit=commit)

import shlex
import datetime
import subprocess

from .common import tqdm, STATUSES


def _ts(*args, commit=True):
    cmd = ['ts'] + [str(a) for a in args]
    if not commit:
        print(' '.join(cmd))
        return None
    p = subprocess.run(cmd, capture_output=True)
    return p.stdout.decode('utf-8').strip()


def job_info(ids, filters):
    info = []
    for l in _ts().splitlines()[1:]:
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


def full_info(ids, filters, extra_func=None, tqdm_disable=False):
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

    info = job_info(ids, filters)
    for i in tqdm(info, disable=tqdm_disable):
        ji = _ts('-i', i['id'])
        start_time = get_time(ji, 'Start time: ')
        end_time = get_time(ji, 'End time: ')
        if not start_time:
            delta = None
        elif not end_time:
            delta = datetime.datetime.now() - start_time
        else:
            delta = end_time - start_time
        new_info = {
            'command': get_line(ji, 'Command: '),
            'slots_required': int(get_line(ji, 'Slots required: ') or 1),
            'gpus_required': int(get_line(ji, 'GPUs required: ') or 0),
            'gpu_ids': get_line(ji, 'GPU IDs: '),
            'enqueue_time': get_time(ji, 'Enqueue time: '),
            'start_time': start_time,
            'end_time': end_time,
            'time_run': delta,
        }
        i.update({k: v for k, v in new_info.items() if v is not None})
        if extra_func:
            extra_func(i)
    return info


def output(id, tail=True):
    if tail:
        return _ts('-t', id)
    return _ts('-c', id)


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

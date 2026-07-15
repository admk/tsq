import argparse
import datetime
import json
import os
import random
import shlex
import subprocess
import time
from pathlib import Path

from . import lifecycle


def now():
    return datetime.datetime.now().isoformat()


def slots_required(meta):
    value = meta.get('slots_required')
    return 1 if value is None else int(value)


def tmux_cmd(args, *tmux_args):
    cmd = [args.command]
    config_file = getattr(args, 'config_file', None)
    if config_file:
        cmd += ['-f', config_file]
    if args.socket_path:
        cmd += ['-S', args.socket_path]
    else:
        cmd += ['-L', args.socket]
    cmd += [str(a) for a in tmux_args]
    return cmd


def tmux(args, *tmux_args, capture_output=True, check=True):
    cmd = tmux_cmd(args, *tmux_args)
    if capture_output:
        p = subprocess.run(cmd, capture_output=True, check=check)
        out = p.stdout.decode('utf-8').strip()
        out += p.stderr.decode('utf-8').strip()
        return out
    subprocess.run(cmd, check=check)
    return ''


def session_exists(args, session):
    result = subprocess.run(
        tmux_cmd(args, 'has-session', '-t', session),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def controller_paths(args):
    root = Path(args.state_dir) / 'controllers'
    return sorted(root.glob('*.json')) if root.exists() else []


def write_controller(meta, path):
    lifecycle.atomic_json(path, meta)


def guard_controllers(args):
    current_time = time.time()
    for path in controller_paths(args):
        try:
            meta = read_meta(path)
        except (OSError, json.JSONDecodeError):
            continue
        if not meta.get('enabled', True):
            continue
        has_restarted = meta.get('last_restart') is not None
        session = meta.get('session')
        name = meta.get('name')
        argv = meta.get('argv') or []
        heartbeat_file = meta.get('heartbeat_file')
        if not session or not name or not argv or not heartbeat_file:
            continue
        heartbeat = Path(heartbeat_file)
        try:
            heartbeat_time = heartbeat.stat().st_mtime
        except OSError:
            heartbeat_time = 0
        try:
            last_restart = float(
                meta.get('last_restart') or meta.get('registered_at') or 0)
            timeout = float(meta.get('timeout', 30))
        except (TypeError, ValueError):
            continue
        stale = current_time - max(heartbeat_time, last_restart) > timeout
        running = session_exists(args, session)
        if running and not stale:
            if heartbeat_time > last_restart and meta.get('restart_count'):
                meta['restart_count'] = 0
                try:
                    write_controller(meta, path)
                except OSError:
                    pass
            continue
        restart_count = int(meta.get('restart_count') or 0)
        backoff = min(timeout, 2 ** min(restart_count, 5))
        if (not running and has_restarted and
                current_time - last_restart < backoff):
            continue
        if running:
            tmux(args, 'kill-session', '-t', session, check=False)
        try:
            tmux(
                args,
                'new-session', '-d', '-s', session,
                '-n', f'controller-{name}',
                '-c', meta.get('cwd') or os.getcwd(),
                shlex.join(str(arg) for arg in argv),
            )
        except (OSError, subprocess.CalledProcessError) as e:
            meta['last_error'] = str(e)
        else:
            meta.pop('last_error', None)
        meta['last_restart'] = current_time
        meta['restart_count'] = restart_count + 1
        if path.exists():
            try:
                write_controller(meta, path)
            except OSError:
                pass


def pane_pid(args, session):
    try:
        pid = tmux(args, 'display-message', '-p', '-t', session, '#{pane_pid}')
        return int(pid) if pid else None
    except (subprocess.CalledProcessError, ValueError):
        return None


def parse_gpu_ids(value):
    if not value:
        return []
    return [
        int(part.strip())
        for part in str(value).split(',')
        if part.strip() and part.strip() != '-1'
    ]


def visible_gpu_ids(args):
    visible = args.visible_gpus or os.environ.get('TS_VISIBLE_DEVICES')
    return parse_gpu_ids(visible)


def query_free_gpus(args):
    cmd = [
        'nvidia-smi',
        '--query-gpu=index,memory.free,memory.total',
        '--format=csv,noheader,nounits',
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, check=True, text=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    visible = set(visible_gpu_ids(args))
    free = []
    seen = 0
    for line in result.stdout.splitlines():
        parts = [part.strip() for part in line.split(',')]
        if len(parts) != 3:
            continue
        try:
            gpu_id = int(parts[0])
            memory_free = float(parts[1])
            memory_total = float(parts[2])
        except ValueError:
            continue
        seen += 1
        if visible and gpu_id not in visible:
            continue
        if memory_total and memory_free > args.gpu_free_perc / 100 * memory_total:
            free.append(gpu_id)
    if seen == 0:
        return None
    if not visible:
        return free
    return [gpu_id for gpu_id in visible_gpu_ids(args) if gpu_id in free]


def jobs_dir(args):
    return Path(args.state_dir) / 'jobs'


def meta_path(args, job_id):
    return jobs_dir(args) / str(job_id) / 'meta.json'


def broker_config_path(args):
    return Path(args.state_dir) / 'broker.json'


def runtime_slots(args):
    path = broker_config_path(args)
    try:
        stat = path.stat()
    except OSError:
        args._runtime_slots_cache = (None, args.slots)
        return args.slots

    signature = (stat.st_mtime_ns, stat.st_size)
    cache = getattr(args, '_runtime_slots_cache', None)
    if cache and cache[0] == signature:
        return cache[1]

    try:
        data = read_meta(path)
        slots = int(data.get('slots', args.slots))
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        slots = args.slots
    args._runtime_slots_cache = (signature, slots)
    return slots


def read_meta(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_meta(meta, path):
    tmp = path.with_name('.{}.{}.tmp'.format(path.name, os.getpid()))
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2, sort_keys=True)
        f.write('\n')
    os.replace(tmp, path)


def all_meta(args):
    root = jobs_dir(args)
    if not root.exists():
        return []
    metas = []
    for path in sorted(root.glob('*/meta.json')):
        try:
            metas.append((path, read_meta(path)))
        except (json.JSONDecodeError, OSError):
            continue
    return metas


def refresh_running(args, path, meta):
    if meta.get('status') == 'merging':
        if lifecycle.refresh_merge(meta, now()):
            write_meta(meta, path)
        return meta
    if meta.get('status') != 'running':
        return meta
    session = meta.get('session')
    if session and session_exists(args, session):
        command_result = lifecycle.command_result(meta)
        if command_result is not None:
            lifecycle.finish_command(
                meta,
                command_result['exitcode'],
                command_result.get('end_time') or now(),
            )
            if meta.get('status') == 'merging':
                lifecycle.refresh_merge(meta, now())
            write_meta(meta, path)
            tmux(args, 'kill-session', '-t', session, check=False)
            return meta
        exitcode = finished_exitcode(meta)
        if exitcode is not None:
            meta.update({
                'status': 'success' if exitcode == 0 else 'failed',
                'exitcode': exitcode,
                'end_time': meta.get('end_time') or now(),
            })
            write_meta(meta, path)
            tmux(args, 'kill-session', '-t', session, check=False)
            return meta
        pid = pane_pid(args, session)
        if pid and meta.get('pid') != pid:
            meta['pid'] = pid
            write_meta(meta, path)
        return meta
    try:
        meta = read_meta(path)
    except OSError:
        return meta
    if meta.get('status') != 'running':
        if meta.get('status') == 'merging':
            if lifecycle.refresh_merge(meta, now()):
                write_meta(meta, path)
        return meta
    command_result = lifecycle.command_result(meta)
    if command_result is not None:
        lifecycle.finish_command(
            meta,
            command_result['exitcode'],
            command_result.get('end_time') or now(),
        )
        if meta.get('status') == 'merging':
            lifecycle.refresh_merge(meta, now())
        write_meta(meta, path)
        return meta
    exitcode = finished_exitcode(meta)
    if exitcode is not None:
        meta.update({
            'status': 'success' if exitcode == 0 else 'failed',
            'exitcode': exitcode,
            'end_time': meta.get('end_time') or now(),
        })
        write_meta(meta, path)
        return meta
    meta.update({
        'status': 'interrupted',
        'exitcode': None,
        'end_time': now(),
    })
    write_meta(meta, path)
    return meta


def finished_exitcode(meta):
    # Merge commands hand off exclusively through command-result.json.  Do
    # not let command output spoof the legacy wrapper completion marker.
    if isinstance(meta.get('merge'), dict):
        return None
    output_file = meta.get('output_file')
    if not output_file:
        return None
    marker = f'[taskq] job {meta["id"]} finished with exit code '
    try:
        with open(output_file, 'rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 65536))
            data = f.read().decode('utf-8', errors='replace')
    except OSError:
        return None
    for line in reversed(data.splitlines()):
        if marker not in line:
            continue
        value = line.split(marker, 1)[1].split(maxsplit=1)[0]
        try:
            return int(value)
        except ValueError:
            return None
    return None


def dependency_state(meta, metas_by_id):
    for dependency in meta.get('depends_on') or []:
        try:
            dependency = int(dependency)
        except (TypeError, ValueError):
            return 'failed', f'invalid dependency {dependency!r}'
        if dependency == int(meta['id']):
            return 'failed', f'job cannot depend on itself ({dependency})'
        dependency_meta = metas_by_id.get(dependency)
        if dependency_meta is None:
            return 'failed', f'dependency {dependency} does not exist'
        status = dependency_meta.get('status')
        if status == 'success':
            continue
        if status in {'failed', 'killed', 'interrupted'}:
            return 'failed', (
                f'dependency {dependency} ended with status {status}')
        return 'pending', None
    return 'ready', None


def mark_dependency_failed(meta, path, reason):
    output_file = meta.get('output_file')
    if output_file:
        try:
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'a', encoding='utf-8') as f:
                f.write(f'[taskq] job {meta["id"]} not started: {reason}\n')
        except OSError:
            pass
    meta.update({
        'status': 'failed',
        'exitcode': None,
        'end_time': now(),
    })
    write_meta(meta, path)
    return meta


def start_job(args, path, meta, gpu_ids=None):
    job_id = meta['id']
    session = meta.get('session') or f'{args.prefix}-{job_id}'
    output_file = meta.get('output_file')
    wrapper = meta.get('wrapper')
    start_file = meta.get('start_file')
    if not wrapper:
        return
    if output_file:
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    meta.update({
        'status': 'running',
        'start_time': now(),
        'session': session,
        'pid': None,
        'gpu_ids': ','.join(str(gpu_id) for gpu_id in gpu_ids or []),
    })
    write_meta(meta, path)
    if gpu_ids:
        taskq_gpu_ids = ','.join(str(gpu_id) for gpu_id in gpu_ids)
    else:
        taskq_gpu_ids = '-1'
    run_command = 'exec "$TASKQ_WRAPPER"'
    try:
        tmux(
            args,
            'new-session', '-d', '-s', session, '-n', f'job-{job_id}',
            '-e', f'TASKQ_GPU_IDS={taskq_gpu_ids}',
            '-e', f'TASKQ_WRAPPER={wrapper}',
            '-c', meta.get('cwd') or os.getcwd(), run_command,
        )
    except subprocess.CalledProcessError as e:
        mark_start_failed(meta, path, e)
        return
    tmux(
        args,
        'set-option', '-t', session, 'history-limit',
        str(args.history_limit),
        check=False,
    )
    if output_file:
        tmux(
            args,
            'pipe-pane', '-o', '-t', f'{session}:0.0',
            f'cat >> {shlex.quote(output_file)}',
            check=False,
        )
    if start_file:
        Path(start_file).touch()
    try:
        current = read_meta(path)
    except OSError:
        return
    if current.get('status') != 'running':
        return
    pid = pane_pid(args, session)
    if pid:
        current['pid'] = pid
        write_meta(current, path)


def mark_start_failed(meta, path, error):
    output_file = meta.get('output_file')
    message = str(error)
    stderr = getattr(error, 'stderr', None)
    if stderr:
        if isinstance(stderr, bytes):
            stderr = stderr.decode('utf-8', errors='replace')
        message += f'\n{stderr.strip()}'
    if output_file:
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(f'[taskq] failed to start tmux session: {message}\n')
    meta.update({
        'status': 'failed',
        'exitcode': getattr(error, 'returncode', None),
        'end_time': now(),
    })
    write_meta(meta, path)


def mark_gpu_unavailable(meta, path):
    output_file = meta.get('output_file')
    gpus_required = int(meta.get('gpus_required') or 0)
    message = (
        '[taskq] GPU allocation requested '
        f'({gpus_required}), but nvidia-smi is not available '
        'or reported no NVIDIA GPUs. '
        'Install NVIDIA GPU tooling or submit the job with -G 0.'
    )
    if output_file:
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(message + '\n')
    meta.update({
        'status': 'failed',
        'exitcode': None,
        'end_time': now(),
    })
    write_meta(meta, path)


def tick(args):
    guard_controllers(args)
    metas = []
    for path, meta in all_meta(args):
        metas.append((path, refresh_running(args, path, meta)))
    metas_by_id = {int(meta['id']): meta for _, meta in metas}

    running_slots = sum(
        slots_required(meta)
        for _, meta in metas
        if meta.get('status') == 'running'
    )
    used_gpu_ids = set()
    for _, meta in metas:
        if meta.get('status') == 'running':
            used_gpu_ids.update(parse_gpu_ids(meta.get('gpu_ids')))
    free_gpu_ids = query_free_gpus(args)
    if free_gpu_ids is not None:
        random.shuffle(free_gpu_ids)
    slots = runtime_slots(args)
    queued = sorted(
        (
            (path, meta)
            for path, meta in metas
            if meta.get('status') == 'queued'
        ),
        key=lambda item: int(item[1].get('id') or 0),
    )
    for path, meta in queued:
        state, reason = dependency_state(meta, metas_by_id)
        if state == 'pending':
            continue
        if state == 'failed':
            meta = mark_dependency_failed(meta, path, reason)
            metas_by_id[int(meta['id'])] = meta
            continue
        required = slots_required(meta)
        can_start = running_slots + required <= slots
        can_oversubscribe = running_slots == 0 and required > slots
        if not can_start and not can_oversubscribe:
            continue
        gpus_required = int(meta.get('gpus_required') or 0)
        gpu_ids = []
        if gpus_required:
            if free_gpu_ids is None:
                mark_gpu_unavailable(meta, path)
                continue
            available = [
                gpu_id
                for gpu_id in free_gpu_ids
                if gpu_id not in used_gpu_ids
            ]
            if len(available) < gpus_required:
                continue
            gpu_ids = available[:gpus_required]
        start_job(args, path, meta, gpu_ids)
        running_slots += required
        used_gpu_ids.update(gpu_ids)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--state-dir', required=True)
    parser.add_argument('--prefix', required=True)
    parser.add_argument('--command', default='tmux')
    parser.add_argument('--config-file', default=None)
    parser.add_argument('--socket', default='taskq')
    parser.add_argument('--socket-path', default=None)
    parser.add_argument('--slots', type=int, default=1)
    parser.add_argument('--history-limit', type=int, default=100000)
    parser.add_argument('--interval', type=float, default=1)
    parser.add_argument('--gpu-free-perc', type=int, default=90)
    parser.add_argument('--visible-gpus', default=None)
    args = parser.parse_args(argv)
    jobs_dir(args).mkdir(parents=True, exist_ok=True)
    while True:
        tick(args)
        time.sleep(args.interval)


if __name__ == '__main__':
    main()

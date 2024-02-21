import os
import sys

from tqdm import tqdm as tqdm_


def tqdm(*args, disable=None, **kwargs):
    return tqdm_(*args, **kwargs, delay=1, disable=disable)


STATUSES = ['running', 'queued', 'success', 'failed', 'killed']

STDIN_TTY = os.isatty(sys.stdin.fileno())
STDOUT_TTY = os.isatty(sys.stdout.fileno())
STDERR_TTY = os.isatty(sys.stderr.fileno())


def tail_lines(text, tail):
    if tail <= 0:
        return text
    return '\n'.join(text.split('\n')[-tail:])


def file_tail_lines(file, tail):
    if isinstance(file, str):
        try:
            with open(file, 'r', encoding='utf-8') as f:
                return tail_lines(f.read(), tail)
        except FileNotFoundError:
            return file
    return tail_lines(file.read(), tail)


def dict_merge(d, u):
    for k, v in u.items():
        if isinstance(v, dict):
            d[k] = dict_merge(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def dict_simplify(d, not_value=False):
    if not isinstance(d, dict):
        return d
    for k, v in list(d.items()):
        v = dict_simplify(v, not_value)
        if v == {} or (not_value and not v):
            del d[k]
        else:
            d[k] = v
    return d

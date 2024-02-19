import os
import sys

from tqdm import tqdm as tqdm_


def tqdm(*args, disable=None, **kwargs):
    return tqdm_(*args, **kwargs, delay=1, disable=disable)


STATUSES = ['running', 'allocating', 'success', 'failed', 'killed']

STDIN_TTY = os.isatty(sys.stdin.fileno())
STDOUT_TTY = os.isatty(sys.stdout.fileno())
STDERR_TTY = os.isatty(sys.stderr.fileno())


def tail_lines(text, tail):
    if tail <= 0:
        return text
    return '\n'.join(text.split('\n')[-tail:])


def file_tail_lines(file, tail):
    if isinstance(file, str):
        with open(file, 'r', encoding='utf-8') as f:
            return tail_lines(f.read(), tail)
    return tail_lines(file.read(), tail)


def unique(seq):
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]

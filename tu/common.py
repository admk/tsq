import os
import sys

from tqdm import tqdm as tqdm_


def tqdm(*args, disable=None, **kwargs):
    return tqdm_(*args, **kwargs, delay=1, disable=disable)


STATUSES = ['running', 'allocating', 'success', 'failed', 'killed']

STDIN_TTY = os.isatty(sys.stdin.fileno())
STDOUT_TTY = os.isatty(sys.stdout.fileno())
STDERR_TTY = os.isatty(sys.stderr.fileno())


def file_tail_lines(f, n):
    if isinstance(f, str):
        with open(f, 'r', encoding='utf-8') as f:
            return file_tail_lines(f, n)
    if n > 0:
        f.seek(0, os.SEEK_END)
        f.seek(max(f.tell() - n, 0), os.SEEK_SET)
    return f.read()


def tail_lines(text, n):
    if n == 0:
        return text
    return ''.join(text.splitlines()[-n:])

import os
import sys

from tqdm import tqdm as tqdm_


def tqdm(*args, disable=None, **kwargs):
    return tqdm_(*args, **kwargs, delay=1, disable=disable)


STATUSES = ['running', 'allocating', 'success', 'failed', 'killed']

STDIN_TTY = os.isatty(sys.stdin.fileno())
STDOUT_TTY = os.isatty(sys.stdout.fileno())
STDERR_TTY = os.isatty(sys.stderr.fileno())

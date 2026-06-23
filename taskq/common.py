import os
import sys
from dataclasses import dataclass
from pathlib import Path

from tqdm import tqdm as tqdm_

from . import TOOL_NAME


def tqdm(*args, disable=None, **kwargs):
    return tqdm_(*args, **kwargs, delay=1, disable=disable)


STATUSES = ['running', 'queued', 'success', 'failed', 'killed', 'interrupted']


def xdg_config_home(environ=None):
    if environ is None:
        environ = os.environ
    value = environ.get('XDG_CONFIG_HOME')
    return Path(os.path.expanduser(value)) if value else None


def user_config_dir(environ=None):
    config_home = xdg_config_home(environ)
    return config_home / TOOL_NAME if config_home else None


def xdg_cache_home(environ=None):
    if environ is None:
        environ = os.environ
    return Path(os.path.expanduser(environ.get('XDG_CACHE_HOME') or '~/.cache'))


def user_cache_dir(environ=None):
    cache_home = xdg_cache_home(environ)
    return cache_home / TOOL_NAME if cache_home else None


def project_config_dir(root=None):
    return Path(root or os.getcwd()) / f'.{TOOL_NAME}'


@dataclass
class FilterArgs:
    force_all: bool = False
    running: bool = False
    queued: bool = False
    success: bool = False
    failed: bool = False
    killed: bool = False
    interrupted: bool = False

    @property
    def all(self):
        flags = all(not getattr(self, a) for a in STATUSES)
        return self.force_all or flags


def isatty(stream):
    try:
        return os.isatty(stream.fileno())
    except (AttributeError, OSError):
        return False


STDIN_TTY = isatty(sys.stdin)
STDOUT_TTY = isatty(sys.stdout)
STDERR_TTY = isatty(sys.stderr)


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

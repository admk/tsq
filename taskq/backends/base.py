import os
import subprocess
from shutil import which
from abc import abstractmethod
from typing import Mapping, Type


class BackendBase:
    def __init__(self, name, config):
        super().__init__()
        self.name = name
        self.config = config
        self.env = self.config.get('env', {})
        self._command = self.config.get('command', name)
        if self._command and not which(self._command):
            raise BackendNotFoundError(
                f'{self._command!r}: command not found')
        slots = self.config.get('slots')
        if slots == 'auto':
            try:
                nv = subprocess.check_output(['nvidia-smi', '-L'])
                slots = len(nv.splitlines())
            except (FileNotFoundError, subprocess.CalledProcessError):
                slots = 1
        self.config['slots'] = slots

    def exec(self, *args, commit=True, shell=False, check=True):
        cmd = [self._command] + [str(a) for a in args]
        if not commit:
            print(' '.join(cmd))
            return None
        env = dict(os.environ, **self.env)
        if shell:
            subprocess.run(' '.join(cmd), shell=True, env=env, check=check)
            return None
        p = subprocess.run(
            cmd, capture_output=True, env=env, check=check)
        out = p.stdout.decode('utf-8').strip()
        out += p.stderr.decode('utf-8').strip()
        return out

    def backend_getset(self, key, value=None):
        pass

    def backend_command(self, command, commit=True):
        return self.exec(*command, commit=commit, check=False)

    @abstractmethod
    def backend_info(self):
        raise NotImplementedError

    @abstractmethod
    def backend_kill(self, args):
        raise NotImplementedError

    @abstractmethod
    def job_info(self, ids=None, filters=None):
        raise NotImplementedError

    @abstractmethod
    def full_info(
        self, ids=None, filters=None, extra_func=None, tqdm_disable=False
    ):
        raise NotImplementedError

    @abstractmethod
    def output(self, info, tail, shell=False):
        raise NotImplementedError

    @abstractmethod
    def add(self, command, gpus, slots, commit=True):
        raise NotImplementedError

    @abstractmethod
    def kill(self, info, commit=True):
        raise NotImplementedError

    @abstractmethod
    def remove(self, info, commit=True):
        raise NotImplementedError


BACKENDS: Mapping[str, Type[BackendBase]] = {}


def register_backend(name):
    def decorator(cls):
        cls.name = name
        BACKENDS[name] = cls
        return cls
    return decorator


class BackendNotFoundError(FileNotFoundError):
    pass

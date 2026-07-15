import os
import subprocess
from shutil import which
from abc import abstractmethod
from typing import Mapping, Type


class BackendBase:
    supports_git_ref = False
    supports_git_merge = False

    def __init__(self, name, config):
        super().__init__()
        self.name = name
        self.config = config
        self.env = self.config.get('env', {})
        self._command = self.config.get('command', name)
        if self._command and not which(self._command):
            raise BackendNotFoundError(
                f'{self._command!r}: command not found')
        slots = self._resolve_slots(self.config.get('slots', 'auto'))
        self.config['slots'] = slots

    @staticmethod
    def _resolve_slots(slots):
        if slots == 'auto':
            try:
                nv = subprocess.check_output(['nvidia-smi', '-L'])
                return len(nv.splitlines())
            except (FileNotFoundError, subprocess.CalledProcessError):
                return 1
        return int(slots)

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

    def interact(self, info):
        return self.output(info, 0, shell=True)

    @abstractmethod
    def backend_info(self):
        raise NotImplementedError

    @abstractmethod
    def backend_kill(self, args):
        raise NotImplementedError

    def backend_reset(self, args):
        pass

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

    def mark_workflow_failed(self, info, reason=None, phase='workflow'):
        """Override a successful managed job with a workflow-level failure."""
        raise NotImplementedError

    @abstractmethod
    def add(
        self, command, gpus=None, slots=None, depends_on=None, merge=None,
        **kwargs,
    ):
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


class BackendError(Exception):
    pass

from abc import abstractmethod

from ..common import dict_simplify


class BackendBase:
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.env = self.config.get('env', {})

    def backend_getset(self, key, value=None):
        pass

    @abstractmethod
    def backend_info(self):
        raise NotImplementedError

    @abstractmethod
    def backend_kill(self, args):
        raise NotImplementedError

    @abstractmethod
    def backend_command(self, command, commit=True):
        raise NotImplementedError

    @abstractmethod
    def job_info(self, ids, filters):
        raise NotImplementedError

    @abstractmethod
    def full_info(self, ids, filters, extra_func=None, tqdm_disable=False):
        raise NotImplementedError

    @abstractmethod
    def output(self, info, tail):
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


BACKENDS = {}


def register_backend(name):
    def decorator(cls):
        cls.name = name
        BACKENDS[name] = cls
        return cls
    return decorator

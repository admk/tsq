import os
from abc import abstractmethod

from ..common import dict_merge


class BackendBase:
    default_config = {
        'group': 'default',
        'slots': 'auto',
    }

    def __init__(self, config):
        super().__init__()
        self.config = dict_merge(self.default_config, config)
        self.env = {}

    @abstractmethod
    def backend_info(self):
        raise NotImplementedError

    @abstractmethod
    def backend_kill(self, args):
        raise NotImplementedError

    @abstractmethod
    def job_info(self, ids, filters):
        raise NotImplementedError

    @abstractmethod
    def full_info(self, ids, filters):
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

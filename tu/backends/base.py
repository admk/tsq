from abc import abstractmethod


class BackendBase:
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
        BACKENDS[name] = cls
        return cls
    return decorator

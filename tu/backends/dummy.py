from .base import register_backend, BackendBase


@register_backend('dummy')
class DummyBackend(BackendBase):
    def backend_info(self):
        return {
            'name': 'dummy',
            'command': None,
            'version': '0.0.0',
        }

    def backend_reset(self, args):
        pass

    def backend_command(self, command, commit=True):
        return command

    def job_info(self, ids, filters):
        return []

    def full_info(self, ids, filters, tqdm_disable=False):
        return []

    def output(self, info, tail):
        pass

    def add(self, command, gpus, slots, commit=True):
        return 0

    def kill(self, info, commit=True):
        pass

    def remove(self, info, commit=True):
        pass

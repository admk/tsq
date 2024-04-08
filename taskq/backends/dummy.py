from .base import register_backend, BackendBase


@register_backend('dummy')
class DummyBackend(BackendBase):
    def __init__(self, name, config):
        config['command'] = None
        super().__init__(name, config)

    def backend_info(self):
        return {
            'name': 'dummy',
            'command': '',
            'version': '0.0.0',
        }

    def backend_kill(self, args):
        print(args)

    def backend_command(self, command, commit=True):
        return command, commit

    def job_info(self, ids=None, filters=None):
        print(ids, filters)
        return []

    def full_info(
        self, ids=None, filters=None, extra_func=None, tqdm_disable=False
    ):
        print(ids, filters)
        return []

    def output(self, info, tail, shell=False):
        print(info, tail, shell)

    def add(self, command, gpus, slots, commit=True):
        print(repr(command), gpus, slots, commit)
        return 0

    def kill(self, info, commit=True):
        print(info, commit)

    def remove(self, info, commit=True):
        print(info, commit)

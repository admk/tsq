from abc import abstractmethod
from typing import Mapping, Type

from ..backends import BACKENDS


class ActionBase:
    def __init__(self, name, parser_kwargs):
        super().__init__()
        self.options = {}
        self.name = name
        self.parser_kwargs = parser_kwargs

    def transform_args(self, args):
        return args

    @abstractmethod
    def main(self, args):
        raise NotImplementedError

    def __call__(self, args, config):
        args = self.transform_args(args)
        backend = config['backend']
        try:
            backend_cls = BACKENDS[backend]
        except KeyError:
            print(f'Invalid backend: {backend}')
            backend_cls = BACKENDS['dummy']
        self.backend = backend_cls(backend, config)
        return self.main(args)


class DryActionBase(ActionBase):
    dry_options = {
        ('-d', '--dry-run'): {
            'action': 'store_true',
            'help': (
                'Do not actually perform the action, '
                'just print what would be done.'),
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.dry_options)

    def transform_args(self, args):
        args = super().transform_args(args)
        args.commit = not args.dry_run
        return args


_actions: Mapping[str, Type[ActionBase]] = {}
_aliases: Mapping[str, str] = {}
INFO = {
    'default': None,
    'actions': _actions,
    'aliases': _aliases,
}


def register_action(name, help=None, aliases=(), default=False):
    def decorator(cls):
        if name in INFO['actions']:
            raise ValueError(f'Action {name!r} already registered.')
        kwargs = {
            'name': name,
            'help': help,
            'aliases': aliases,
        }
        INFO['actions'][name] = cls(name, kwargs)
        INFO['aliases'].update({a: name for a in list(aliases) + [name]})
        if default:
            if INFO['default'] is not None:
                raise ValueError(
                    f'Default action already set to {INFO["default"]!r}.')
            INFO['default'] = name
        return cls
    return decorator

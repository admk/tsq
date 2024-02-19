import copy
from abc import abstractmethod

import tomlkit

from ..backends import BACKENDS


class ActionBase:
    base_options = {
        ('-rc', '--rc-file'): {
            'type': str,
            'default': None,
            'help':
                'The configuration file to use.'
                'If not provided, it reads from "~/.config/tu.toml" '
                'and "./.tu.toml".'
        },
        ('-b', '--backend'): {
            'type': str,
            'default': None,
            'choices': ['ts'],
            'help': 'The backend to use.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__()
        self.options = copy.deepcopy(self.base_options)
        self.name = name
        self.parser_kwargs = parser_kwargs

    def transform_args(self, args):
        return args

    @abstractmethod
    def main(self, args):
        raise NotImplementedError

    def _load_config(self, args):
        if args.rc_file:
            with open(args.rc_file, 'r', encoding='utf-8') as f:
                return tomlkit.load(f)
        config = {}
        for path in ('~/.config/tu.toml', './.tu.toml'):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    config.update(tomlkit.load(f))
            except FileNotFoundError:
                pass
        return config

    def __call__(self, args):
        args = self.transform_args(args)
        config = self._load_config(args)
        backend_name = args.backend or config.get('backend', 'ts')
        try:
            backend_cls = BACKENDS[backend_name]
        except KeyError:
            print(f'Invalid backend: {args.backend}')
            return 1
        self.backend = backend_cls(config)
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


INFO = {
    'default': None,
    'actions': {},
    'aliases': {},
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

import os
import copy
from abc import abstractmethod

import tomlkit

from ..backends import BACKENDS
from ..common import dict_merge


class ActionBase:
    base_options = {
        ('-rc', '--rc-file'): {
            'type': str,
            'default': None,
            'help':
                'The configuration file to use. '
                'If not provided, it reads from "~/.config/tu.toml" '
                'and "./.tu.toml".'
        },
        ('-b', '--backend'): {
            'type': str,
            'default': None,
            'choices': list(BACKENDS.keys()),
            'help': 'The backend to use.',
        },
        ('-g', '--group'): {
            'type': str,
            'default': None,
            'help': 'The group to use.',
        },
    }
    rc_path = '.tu.toml'

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
        else:
            args.rc_file = self.rc_path
        rc_files = [
            os.path.join(os.path.dirname(__file__), '..', 'default.toml'),
            '~/.config/tu.toml',
            self.rc_path,
        ]
        config = {}
        for path in rc_files:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    config = dict_merge(config, tomlkit.load(f))
            except FileNotFoundError:
                pass
        return config

    def _update_config(self, config, backend, group):
        backend_config = config.get('backends', {}).get(backend, {})
        group_config = config.get('groups', {}).get(group, {})
        dict_merge(config, backend_config)
        dict_merge(config, group_config)
        config.update({
            'backend': backend,
            'group': group,
        })
        return config

    def __call__(self, args):
        args = self.transform_args(args)
        config = self._load_config(args)
        backend_name = args.backend or config.get('backend', 'ts')
        group_name = args.group or config.get('group', 'default')
        config = self._update_config(config, backend_name, group_name)
        try:
            backend_cls = BACKENDS[backend_name]
        except KeyError:
            print(f'Invalid backend: {backend_name}')
            backend_cls = BACKENDS['dummy']
        self.backend = backend_cls(config) if backend_cls else None
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

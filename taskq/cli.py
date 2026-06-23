import sys
import argparse
from importlib import resources

import tomlkit

from . import TOOL_NAME, __version__
from .common import dict_merge, project_config_dir, user_config_dir
from .actions import INFO
from .backends import BACKENDS


class CLI:
    base_options = {
        ('-V', '--version'): {
            'action': 'store_true',
            'help': 'print the version and exit',
        },
        ('-rc', '--rc-file'): {
            'type': str,
            'default': None,
            'help':
                'The configuration file to use. '
                f'If not provided, it reads from '
                f'$XDG_CONFIG_HOME/{TOOL_NAME}/config.toml '
                f'and ./.{TOOL_NAME}/config.toml.'
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
    rc_path = f'.{TOOL_NAME}/config.toml'
    default_config = 'default.toml'

    def __init__(self):
        super().__init__()
        self.parser = argparse.ArgumentParser()
        for option, kwargs in self.base_options.items():
            self.parser.add_argument(*option, **kwargs)
        action_parsers = self.parser.add_subparsers(dest='action')
        for action in INFO['actions'].values():
            kwargs = action.parser_kwargs
            action_parser = action_parsers.add_parser(**kwargs)
            for option, kwargs in action.options.items():
                action_parser.add_argument(*option, **kwargs)

    def _load_config(self, args):
        config = {}
        try:
            config = tomlkit.loads(
                resources.files('taskq').joinpath(
                    self.default_config).read_text(encoding='utf-8')
            )
        except (FileNotFoundError, tomlkit.exceptions.ParseError) as e:
            print(f'Error parsing default config: {e}')
        if args.rc_file:
            with open(args.rc_file, 'r', encoding='utf-8') as f:
                try:
                    return dict_merge(config, tomlkit.load(f))
                except tomlkit.exceptions.ParseError as e:
                    print(f'Error parsing {args.rc_file}: {e}')
                    return config
        project_path = project_config_dir() / 'config.toml'
        args.rc_file = str(project_path)
        rc_files = []
        config_dir = user_config_dir()
        if config_dir:
            rc_files.append(config_dir / 'config.toml')
        rc_files.append(project_path)
        for path in rc_files:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    try:
                        rc = tomlkit.load(f)
                    except tomlkit.exceptions.ParseError as e:
                        print(f'Error parsing {path}: {e}')
                        continue
                config = dict_merge(config, rc)
            except FileNotFoundError:
                pass
        return config

    def _resolve_config(self, args, config):
        backend = args.backend or config.get('backend', 'tmux')
        group = args.group or config.get('group', 'default')
        backend_config = config.get('backends', {}).get(backend, {})
        group_config = config.get('groups', {}).get(group, {})
        dict_merge(config, backend_config)
        dict_merge(config, group_config)
        config.update({
            'backend': backend,
            'group': group,
        })
        return config

    def main(self, args=None):
        args = args or sys.argv[1:]
        if all(a not in INFO['aliases'] for a in args):
            if '-h' in args or '--help' in args:
                self.parser.print_help()
                sys.exit(0)
            if '-V' in args or '--version' in args:
                print(__version__)
                sys.exit(0)
            args = [INFO['default']] + args
        args = self.parser.parse_args(args)
        config = self._load_config(args)
        config = self._resolve_config(args, config)
        try:
            action = INFO['aliases'][args.action]
            action_func = INFO['actions'][action]
        except KeyError:
            print(f'Invalid action: {args.action}')
            sys.exit(1)
        return action_func(args, config)


def main():
    sys.exit(CLI().main())


if __name__ == '__main__':
    main()

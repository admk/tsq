
from abc import abstractmethod


class ActionBase:
    name = NotImplemented

    def __init__(self, name, parser_kwargs):
        super().__init__()
        self.parser_kwargs = parser_kwargs

    def add_arguments(self, parser):
        pass

    def transform_args(self, args):
        return args

    @abstractmethod
    def main(self, args):
        raise NotImplementedError

    def __call__(self, args):
        args = self.transform_args(args)
        return self.main(args)


class DryActionBase(ActionBase):
    def add_arguments(self, parser):
        help_message = (
            'Do not actually perform the action, '
            'just print what would be done.')
        parser.add_argument(
            '-d', '--dry-run', action='store_true', help=help_message)

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

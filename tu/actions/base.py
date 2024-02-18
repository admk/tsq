
from abc import abstractmethod


class ActionBase:
    name = NotImplemented
    help = NotImplemented

    def __init__(self, name, help_message):
        super().__init__()
        self.name = name
        self.help = help_message

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


DEFAULT_ACTION = 'ls'
ACTIONS = {}

def register_action(name, help_message, default=False):
    def decorator(cls):
        if name in ACTIONS:
            raise ValueError(f'Action {name!r} already registered.')
        ACTIONS[name] = cls(name, help_message)
        if default:
            global DEFAULT_ACTION
            DEFAULT_ACTION = name
        return cls
    return decorator

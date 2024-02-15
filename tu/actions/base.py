
from abc import abstractmethod


class ActionBase:
    name = NotImplemented
    help = NotImplemented

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


ACTIONS = {}

def register_action(name, help_message):
    def decorator(cls):
        cls.name = name
        cls.help = help_message
        ACTIONS[name] = cls()
        return cls
    return decorator

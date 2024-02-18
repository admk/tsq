import sys
import argparse

from .actions import INFO


class CLI:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.action_parsers = self.parser.add_subparsers(dest='action')
        for action in INFO['actions'].values():
            self.add_action(action)

    def add_action(self, action):
        kwargs = action.parser_kwargs
        action_parser = self.action_parsers.add_parser(**kwargs)
        action.add_arguments(action_parser)

    def main(self, args=None):
        args = args or sys.argv[1:]
        if all(a not in INFO['aliases'] for a in args):
            if '-h' in args or '--help' in args:
                self.parser.print_help()
                sys.exit(0)
            args = [INFO['default']] + args
        args = self.parser.parse_args(args)
        try:
            action = INFO['aliases'][args.action]
            action_func = INFO['actions'][action]
        except KeyError:
            print(f'Invalid action: {args.action}')
            sys.exit(1)
        return action_func(args)


def main():
    sys.exit(CLI().main())


if __name__ == '__main__':
    main()

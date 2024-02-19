import sys
import argparse

from .actions import INFO


class CLI:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.action_parsers = self.parser.add_subparsers(dest='action')
        self.init_parser()

    def init_parser(self):
        for action in INFO['actions'].values():
            kwargs = action.parser_kwargs
            action_parser = self.action_parsers.add_parser(**kwargs)
            for option, kwargs in action.options.items():
                action_parser.add_argument(*option, **kwargs)

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

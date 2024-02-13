import sys
import argparse

from .actions import ACTIONS


class CLI:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.action_parsers = self.parser.add_subparsers(dest='action')
        for action in ACTIONS.values():
            self.add_action(action)

    def add_action(self, action):
        action_parser = self.action_parsers.add_parser(
            action.name, help=action.help)
        action.add_arguments(action_parser)

    def main(self, args=None):
        args = self.parser.parse_args(args)
        try:
            action = ACTIONS[args.action]
        except KeyError:
            print(f'Invalid action: {args.action}')
            sys.exit(1)
        return action(args)


def main():
    sys.exit(CLI().main())


if __name__ == '__main__':
    main()

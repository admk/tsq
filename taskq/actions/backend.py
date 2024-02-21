import argparse

import tomlkit

from ..common import tqdm, FilterArgs
from .base import register_action, ActionBase


@register_action('backend', 'backend actions', aliases=['b'])
class BackendAction(ActionBase):
    backend_options = {
        ('backend_action', ): {
            'type': str,
            'default': None,
            'choices': ['info', 'reset', 'command'],
            'help': 'The action to perform.',
        },
        ('command', ): {
            'type': str,
            'nargs': argparse.REMAINDER,
            'help': 'The command to run.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.backend_options)

    def info(self, args):
        binfo = self.backend.backend_info()
        print(tomlkit.dumps(binfo).rstrip())

    def reset(self, args):
        info = self.backend.full_info(None, FilterArgs())
        for i in tqdm(info):
            self.backend.remove(i)
        self.backend.backend_kill(args)
        print(f'Killed {self.backend.name} backend.')

    def command(self, args):
        if not args.command:
            print('No command provided.')
            return 1
        print(self.backend.backend_command(args.command))

    def main(self, args):
        try:
            func = getattr(self, args.backend_action)
        except AttributeError:
            print(f'Invalid backend action: {args.backend_action}')
            return 1
        return func(args)

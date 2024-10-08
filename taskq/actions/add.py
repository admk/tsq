import re
import sys
import string
import random
import textwrap
import argparse
import itertools

from ..common import tqdm, STDIN_TTY, FilterArgs
from .base import register_action, DryActionBase


@register_action('add', 'add jobs', aliases=['a'])
class AddAction(DryActionBase):
    add_options = {
        ('-G', '--gpus'): {
            'type': int,
            'default': None,
            'help': 'Number of GPUs required.',
        },
        ('-N', '--slots'): {
            'type': int,
            'default': None,
            'help': 'Number of slots required.',
        },
        ('-u', '--unique'): {
            'action': 'store_true',
            'help':
                'Only add unique commands. '
                'If a command is already in the queue, '
                'it will also be skipped.',
        },
        ('-i', '--interact'): {
            'action': 'store_true',
            'help': 'Interact with the added command that is running.',
        },
        ('-f', '--from-file'): {
            'type': str,
            'default': None,
            'help':
                'Read commands from a file, one per line. '
                'If "-", read from standard input.',
        },
        ('-s', '--separator'): {
            'type': str,
            'default': ',',
            'help': 'Separator for arguments from standard input.',
        },
        ('command', ): {
            'type': str,
            'nargs': argparse.REMAINDER,
            'help': 'The command to run.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.add_options)

    @staticmethod
    def _extrapolate_inputs(command, from_file, sep=','):
        if not STDIN_TTY:
            inputs = sys.stdin.read().split('\n')
        else:
            inputs = []
        if from_file == '-':
            commands = inputs
        elif from_file:
            with open(from_file, 'r', encoding='utf-8') as f:
                commands = f.read().split('\n')
        else:
            commands = []
        if command:
            commands += [' '.join(command)]
        if from_file == '-':
            # if we read from stdin for commands,
            # we can't read from stdin again for arguments
            return commands
        if not inputs:
            # nothing in stdin, so we can't extrapolate arguments
            return commands
        new_commands = []
        for line in inputs:
            line = line.strip()
            if not line:
                continue
            args = line.split(sep)
            for c in commands:
                # order reversed to avoid replacing "@1" in "@10"
                for j, a in reversed(list(enumerate(args))):
                    c = c.replace(f'@{j + 1}', a.strip())
                new_commands.append(c)
        return new_commands

    @staticmethod
    def _regex_extrapolate(texts, regex, extrapolator):
        new_texts = []
        for text in texts:
            scope_regex = re.compile(regex)
            scopes = scope_regex.findall(text)
            if not scopes:
                new_texts.append(text)
                continue
            values = itertools.product(*[extrapolator(s) for s in scopes])
            for v in values:
                replacer = lambda m, i=iter(v): str(next(i))
                h = scope_regex.sub(replacer, text)
                new_texts.append(h)
        return new_texts

    @classmethod
    def _extrapolate_ranges(cls, commands):
        def extrapolator(s):
            values = []
            for r in s[0].split(','):
                if not r:
                    continue
                if '-' in r:
                    start, end = r.split('-')
                    values += list(range(int(start), int(end) + 1))
                else:
                    values.append(int(r))
            return values
        regex = r'\[((?:\d+(?:-\d+)?)(?:,(?:\d+(-\d+)?))*)\]'
        return cls._regex_extrapolate(commands, regex, extrapolator)

    @classmethod
    def _extrapolate_sets(cls, commands):
        return cls._regex_extrapolate(
            commands, r'\{([^}]+)\}', lambda s: s.split(','))

    @staticmethod
    def _extrapolate_unique_id(commands):
        # add a unique identifier for each command
        alphabet = string.ascii_letters + string.digits
        new_commands = []
        for c in commands:
            rand = random.Random(c)
            uid = ''.join(rand.choice(alphabet) for i in range(8))
            c = c.replace('@u', uid)
            new_commands.append(c)
        return new_commands

    def main(self, args):
        commands = self._extrapolate_inputs(
            args.command, args.from_file, args.separator)
        commands = self._extrapolate_ranges(commands)
        commands = self._extrapolate_sets(commands)
        commands = self._extrapolate_unique_id(commands)
        commands = [c.strip() for c in commands if c.strip()]
        if args.unique:
            info = self.backend.full_info(None, FilterArgs())
            queued_commands = [i['command'] for i in info]
            commands = list(dict.fromkeys(commands))
            skipped = [c for c in commands if c in queued_commands]
            commands = [c for c in commands if c not in queued_commands]
        else:
            skipped = []
        if not commands:
            print('No command to add.')
            if STDIN_TTY:
                print('Use "-f -" to read commands from stdin.')
            return
        ids = []
        for c in tqdm(commands, desc='add'):
            output = self.backend.add(
                c, args.gpus, args.slots, commit=args.commit)
            ids.append(output)
        if any(ids):
            print('Added:', ', '.join(ids))
        if skipped:
            print('Skipped commands:')
            print(textwrap.indent('\n'.join(skipped), '  '))

import re
import os
import sys
import itertools
import argparse

from ..wrapper import tqdm, add
from .base import register_action, DryActionBase


@register_action('add', 'add jobs')
class AddAction(DryActionBase):
    add_options = {
        ('-G', '--gpus'): {
            'type': int,
            'default': 0,
            'help': 'Number of GPUs required.',
        },
        ('-N', '--slots'): {
            'type': int,
            'default': 1,
            'help': 'Number of slots required.',
        },
        ('-f', '--from-file'): {
            'type': str,
            'default': None,
            'help':
                'Read commands from a file, one per line. '
                'If "-", read from standard input.',
        },
        ('command', ): {
            'type': str,
            'nargs': argparse.REMAINDER,
            'help': 'The command to run.',
        },
    }

    def add_arguments(self, parser):
        super().add_arguments(parser)
        for option, kwargs in self.add_options.items():
            parser.add_argument(*option, **kwargs)

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
    def _extrapolate_inputs(commands, inputs):
        new_commands = []
        for line in inputs:
            line = line.strip()
            if not line:
                continue
            args = line.split(',')
            for c in commands:
                for j, a in enumerate(args):
                    c = c.replace(f'@{j + 1}', a)
                new_commands.append(c)
        return new_commands

    def main(self, args):
        if not os.isatty(sys.stdin.fileno()):
            inputs = sys.stdin.readlines()
        else:
            inputs = []
        if args.from_file == '-':
            commands = inputs
        elif args.from_file:
            with open(args.from_file, 'r', encoding='utf-8') as f:
                commands = f.readlines()
        else:
            commands = []
        if args.command:
            commands += [' '.join(args.command)]
        if not args.from_file and inputs:
            commands = self._extrapolate_inputs(commands, inputs)
        commands = self._extrapolate_ranges(commands)
        commands = self._extrapolate_sets(commands)
        iterer = tqdm(commands, commit=args.commit)
        ids = []
        for c in iterer:
            iterer.set_description(f'Adding {c!r}')
            output = add(c, args.gpus, args.slots, commit=args.commit)
            ids.append(output)
        if any(ids):
            print('Added:', ', '.join(ids))

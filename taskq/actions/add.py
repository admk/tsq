import os
import re
import shlex
import sys
import string
import random
import textwrap
import argparse
import itertools

from ..common import STDIN_TTY, FilterArgs
from .base import register_action, DryActionBase
from .base import CLIError
from .filter import parse_id_selector
from .repeat import (
    AddRequest,
    add_repeated,
    dry_add_command,
    repeat_options,
    resolve_backend_merge_spec,
    validate_repeat_count,
)


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
        ('-D', '--depends-on'): {
            'type': str,
            'default': None,
            'help': (
                'Only start after these job IDs complete successfully. '
                'Accepts comma-separated IDs and ranges, e.g. "1-3,5".'),
        },
        ('-r', '--ref'): {
            'type': str,
            'default': None,
            'help': (
                'Run the job from this git branch, tag, or commit. '
                'The ref is resolved to an exact commit when queued.'),
        },
        ('-m', '--merge'): {
            'type': str,
            'default': None,
            'metavar': 'BRANCH',
            'help': (
                'Merge successful job changes back into the destination '
                'branch. If the branch does not exist, create it from HEAD.'),
        },
        **repeat_options,
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

    def transform_args(self, args):
        args = super().transform_args(args)
        args.depends_on = (
            parse_id_selector(args.depends_on)
            if args.depends_on else []
        )
        args.implicit_merge_head = args.merge is not None and args.ref is None
        if args.implicit_merge_head:
            args.ref = 'HEAD'
        args.repeat = validate_repeat_count(args.repeat)
        return args

    @staticmethod
    def _extrapolate_inputs(command, from_file, sep=','):
        if not STDIN_TTY:
            inputs = [
                line for line in sys.stdin.read().split('\n')
                if line.strip()
            ]
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
            commands += [shlex.join(command)]
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

    @staticmethod
    def _resolved_alloc(config, args):
        alloc_config = config.get('alloc', {})
        gpus = args.gpus if args.gpus is not None else alloc_config.get('gpus', 0)
        slots = (
            args.slots if args.slots is not None
            else alloc_config.get('slots', 1)
        )
        return gpus, slots

    def _validate_ref_support(self, ref):
        if ref and not getattr(self.backend, 'supports_git_ref', False):
            raise CLIError(
                f"backend {self.backend.name!r} does not support --ref")

    def _resolve_ref(self, ref, merge_spec=None):
        self._validate_ref_support(ref)
        if ref and isinstance(merge_spec, dict):
            git_commit = merge_spec.get('source_head')
            git_root = merge_spec.get('repo_root')
            if git_commit and git_root:
                return {
                    'git_ref': ref,
                    'git_commit': git_commit,
                    'git_root': git_root,
                    'source_cwd': (
                        merge_spec.get('source_cwd') or os.getcwd()),
                }
        if ref and hasattr(self.backend, 'resolve_git_ref'):
            git_root, git_commit = self.backend.resolve_git_ref(ref)
            return {
                'git_ref': ref,
                'git_commit': git_commit,
                'git_root': git_root,
                'source_cwd': os.getcwd(),
            }
        if ref:
            return {'git_ref': ref}
        return {}

    @staticmethod
    def _request_kwargs(ref_kwargs, merge_kwargs=None):
        return dict(ref_kwargs, **(merge_kwargs or {}))

    def _resolve_merge(self, branch, commit=True):
        if branch is None:
            return {}
        return {
            'merge': resolve_backend_merge_spec(
                self.backend, branch, create=commit),
        }

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
            if skipped:
                print('Skipped commands:')
                print(textwrap.indent('\n'.join(skipped), '  '))
                return
            print('No command to add.')
            if STDIN_TTY:
                print('Use "-f -" to read commands from stdin.')
            return
        self._validate_ref_support(args.ref)
        if args.implicit_merge_head:
            merge_kwargs = self._resolve_merge(
                args.merge, commit=args.commit)
            ref_kwargs = self._resolve_ref(
                args.ref, merge_spec=merge_kwargs.get('merge'))
        else:
            ref_kwargs = self._resolve_ref(args.ref)
            merge_kwargs = self._resolve_merge(
                args.merge, commit=args.commit)
        if not args.commit:
            gpus, slots = self._resolved_alloc(self.backend.config, args)
            requests = [
                AddRequest(
                    c, gpus, slots, args.depends_on,
                    kwargs=self._request_kwargs(ref_kwargs, merge_kwargs),
                )
                for c in commands
            ]
            dry_commands = []
            add_repeated(
                self.backend, requests, args.repeat, commit=False,
                dry_run=lambda request, depends_on: dry_commands.append(
                    dry_add_command(
                        request.command,
                        request.gpus,
                        request.slots,
                        depends_on,
                        request.kwargs.get('git_ref'),
                        args.merge,
                    )
                ),
                chain=args.chain,
            )
            print('\n'.join(dry_commands))
            if skipped:
                print('Skipped commands:')
                print(textwrap.indent('\n'.join(skipped), '  '))
            return
        requests = [
            AddRequest(
                c, args.gpus, args.slots, args.depends_on,
                kwargs=self._request_kwargs(ref_kwargs, merge_kwargs),
            )
            for c in commands
        ]
        id_groups = add_repeated(
            self.backend, requests, args.repeat, commit=args.commit,
            chain=args.chain)
        ids = [job_id for group in id_groups for job_id in group]
        if any(ids):
            print('Added:', ', '.join(ids))
        if args.interact and len(ids) > 1:
            print('Cannot interact with multiple added jobs.')
        elif args.interact and ids and args.commit:
            self.backend.output({'id': int(ids[0])}, 0, shell=True)
        if skipped:
            print('Skipped commands:')
            print(textwrap.indent('\n'.join(skipped), '  '))

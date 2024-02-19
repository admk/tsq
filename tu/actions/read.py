import sys
import time
import platform
from datetime import datetime
from abc import abstractmethod

import tabulate
import textwrap
import functools
from blessed import Terminal

from ..utils import timedelta_format
from ..wrapper import job_info, full_info, output
from .base import register_action
from .filter import FilterActionBase


term = Terminal()


class ReadActionBase(FilterActionBase):
    read_options = {
        ('-n', '--interval'): {
            'type': int,
            'default': None,
            'help': 'Refresh interval in seconds.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.read_options)

    @abstractmethod
    def format(self, args, tqdm_disable=False):
        raise NotImplementedError

    def loop(self, args):
        with term.hidden_cursor(), term.fullscreen():
            while True:
                try:
                    query_start = time.time()
                    print(term.move(0, 0), end='')
                    dt = datetime.fromtimestamp(query_start)
                    print(f'{platform.node()}\t\t\t{dt:%Y-%m-%d %H:%M:%S}')
                    output = self.format(args, tqdm_disable=True)
                    if output:
                        print(output, end=term.clear_eol)
                    print(term.clear_eos, end='')
                    sys.stdout.flush()
                    query_duration = time.time() - query_start
                    sleep_duration = args.interval - query_duration
                    if sleep_duration > 0:
                        time.sleep(sleep_duration)
                except KeyboardInterrupt:
                    return 0

    def main(self, args):
        if args.interval is not None:
            return self.loop(args)
        print(self.format(args, tqdm_disable=False))
        return 0


@register_action(
    'list', 'show job infos in compact format',
    aliases=['ls'], default=True)
class ListAction(ReadActionBase):
    list_options = {
        ('-l', '--length'): {
            'type': int,
            'default': 30,
            'help': 'Max length for each column.',
        },
        ('-c', '--columns'): {
            'type': str,
            'default': 'id,status,start,time,command',
            'help':
                'Columns to display, separated by commas. Available columns: '
                'id, status, slots, gpus, gpu_ids, '
                'enqueue, start, end, time, command.'
        },
        ('-t', '--table-format'): {
            'type': str,
            'default': 'rounded_outline',
            'help': 'The format of the table.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.list_options)

    @staticmethod
    def shorten(text, max_len):
        return textwrap.shorten(text, width=max_len, placeholder='...')

    def format(self, args, tqdm_disable=False):
        info = full_info(self.ids, self.filters, tqdm_disable=tqdm_disable)
        if not info:
            return 'No jobs found.'
        rows = []
        columns = [c.lower() for c in args.columns.split(',')]
        for i in info:
            enqueue_time = i['enqueue_time'].strftime('%m-%d %H:%M')
            try:
                start_time = i['start_time'].strftime('%m-%d %H:%M')
            except KeyError:
                start_time = ''
            try:
                end_time = i['end_time'].strftime('%m-%d %H:%M')
            except KeyError:
                end_time = ''
            try:
                time_run = timedelta_format(i['time_run'], 'wdhms', 2)
            except KeyError:
                time_run = ''
            row = {
                'ID': i['id'],
                'Status': i['status'],
                'Slots': i.get('slots_required', 1),
                'GPUs': i.get('gpus_required', 0),
                'GPU IDs': i.get('gpu_ids', ''),
                'Enqueue': enqueue_time,
                'Start': start_time,
                'End': end_time,
                'Time': time_run,
                'Command': self.shorten(i['command'], args.length),
            }
            rows.append({
                k: v for k, v in row.items()
                if k.replace(' ', '_').lower() in columns})
        table = tabulate.tabulate(
            rows, headers='keys', tablefmt=args.table_format)
        return table


@register_action('ids', help='show job IDs', aliases=['id'])
class IdsAction(ReadActionBase):
    def format(self, args, tqdm_disable=False):
        jobs = job_info(self.ids, self.filters)
        return ', '.join(str(i['id']) for i in jobs)


@register_action('info', 'show job infos')
class InfoAction(ReadActionBase):
    def format(self, args, tqdm_disable=False):
        info = full_info(self.ids, self.filters, tqdm_disable=tqdm_disable)
        if not info:
            return 'No jobs found.'
        outputs = []
        for i in info:
            outputs.append(f'Job {i["id"]}:')
            for k, v in i.items():
                if k == 'id':
                    continue
                outputs.append(f'  {k}: {v}')
        return '\n'.join(outputs)


@register_action('commands', 'show job commands', aliases=['cmd'])
class CommandsAction(ReadActionBase):
    commands_options = {
        ('-j', '--no-job-ids'): {
            'action': 'store_true',
            'help': 'Do not print the job ID before the command.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.commands_options)

    def format(self, args, tqdm_disable=False):
        info = full_info(self.ids, self.filters, tqdm_disable=tqdm_disable)
        outputs = []
        for i in info:
            if args.no_job_ids:
                outputs.append(i['command'])
            else:
                outputs.append(f"{i['id']}: {i['command']}")
        return '\n'.join(outputs)


@register_action('export', 'show job infos in machine-readable format')
class ExportAction(ReadActionBase):
    export_options = {
        ('-e', '--export-format'): {
            'choices': ['json', 'yaml', 'toml'],
            'default': 'json',
            'help': 'The format of the output.',
        },
        ('-t', '--tail'): {
            'action': 'store_true',
            'help': '"tail -n 10 -f" the output of the job.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.export_options)

    def extra_func(self, i, args):
        i['time_run'] = i['time_run'].total_seconds()
        if args.tail:
            i['output'] = output(i['id'], tail=True)

    def format(self, args, tqdm_disable=False):
        extra_func = functools.partial(self.extra_func, args=args)
        info = full_info(
            self.ids, self.filters,
            extra_func=extra_func, tqdm_disable=tqdm_disable)
        if args.export_format == 'json':
            import json
            return json.dumps(info, indent=4, default=str)
        elif args.export_format == 'yaml':
            import yaml
            return yaml.dump(info, default_flow_style=False)
        elif args.export_format == 'toml':
            import toml
            return toml.dumps({'job': info})
        else:
            raise ValueError(f'Unknown format: {args.export_format}')


@register_action('outputs', 'Show job outputs', aliases=['out'])
class OutputsAction(ReadActionBase):
    outputs_options = {
        ('-l', '--lines'): {
            'type': int,
            'default': 10,
            'help': 'Number of lines.',
        },
        ('-R', '--raw'): {
            'action': 'store_true',
            'help': 'Do not format lines.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.outputs_options)

    def format(self, args, tqdm_disable=False):
        info = job_info(self.ids, self.filters)
        outputs = []
        for i in info:
            outputs.append(f'Job {i["id"]}:')
            out = output(i['id'])
            if not args.raw:
                out = '\n'.join(textwrap.wrap(out, width=80))
                out = textwrap.indent(out, '> ') + '\n'
            outputs.append(out)
        return '\n'.join(outputs)

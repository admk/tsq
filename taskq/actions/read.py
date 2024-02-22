import sys
import time
import platform
import textwrap
import functools
from datetime import datetime
from abc import abstractmethod

import tabulate
from blessed import Terminal

from ..common import tqdm, FilterArgs
from ..utils import timedelta_format
from .base import register_action
from .filter import FilterActionBase


term = Terminal()
COLOR_STATUS = {
    'running': term.green,
    'queued': term.yellow,
    'failed': term.red,
    'killed': term.orange,
    'success': term.blue,
}


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
                    table = self.format(args, tqdm_disable=True).split('\n')
                    table = f'{term.clear_eol}\n'.join(table)
                    print(term.move(0, 0), end='')
                    dt = datetime.fromtimestamp(query_start)
                    spaces = ' ' * 12
                    header = f'{platform.node()}{spaces}{dt:%Y-%m-%d %H:%M:%S}'
                    print(header + term.clear_eol)
                    if table:
                        print(table, end=term.clear_eol)
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
                'enqueue, start, end, time, command, output. '
                'Alternatively, prefix a column with "+" to show it, '
                'and with "-" to hide it.'
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
        if not max_len:
            return text
        return text if len(text) <= max_len else text[:max_len - 3] + '...'

    @staticmethod
    def _color_status(status):
        return COLOR_STATUS[status](status).replace('\x1b(B', '')

    @staticmethod
    def _format_time(dt):
        return dt.strftime('%m-%d %H:%M') if dt else None

    def format(self, args, tqdm_disable=False):
        info = self.backend.full_info(
            self.ids, self.filters, tqdm_disable=tqdm_disable)
        if not info:
            return 'No jobs found.'
        rows = []
        default_columns = ['id', 'status', 'start', 'time', 'command']
        columns = [c.lower() for c in args.columns.split(',')]
        plus_columns = [c for c in columns if c.startswith('+')]
        minus_columns = [c for c in columns if c.startswith('-')]
        columns = [c for c in columns if c not in plus_columns + minus_columns]
        if columns and (plus_columns or minus_columns):
            print('Cannot mix +/- columns with explicit columns.')
            return 1
        else:
            columns = default_columns + [c[1:] for c in plus_columns]
            columns = [c for c in columns if f'-{c}' not in minus_columns]
        columns = list(dict.fromkeys(columns))
        for i in info:
            try:
                time_run = timedelta_format(i['time_run'], 'wdhms', 2)
            except KeyError:
                time_run = ''
            row = {
                'ID': i['id'],
                'Status': self._color_status(i['status']),
                'Slots': i.get('slots_required', 1),
                'GPUs': i.get('gpus_required', 0),
                'GPU IDs': i.get('gpu_ids', ''),
                'Enqueue': self._format_time(i.get('enqueue_time')),
                'Start': self._format_time(i.get('start_time')),
                'End': self._format_time(i.get('end_time')),
                'Time': time_run,
                'Command': self.shorten(i['command'], args.length),
            }
            if 'output' in columns:
                if i['status'] not in ['queued']:
                    out = self.backend.output(i, 1)
                else:
                    out = ''
                row['Output'] = self.shorten(out, args.length)
            rows.append({
                k: v for k, v in row.items()
                if k.replace(' ', '_').lower() in columns})
        maxcolwidths = [
            None if c not in ['output', 'command'] else args.length
            for c in columns]
        table = tabulate.tabulate(
            rows, headers='keys', tablefmt=args.table_format,
            maxcolwidths=maxcolwidths)
        return table


@register_action('ids', help='show job IDs', aliases=['id'])
class IdsAction(ReadActionBase):
    def format(self, args, tqdm_disable=False):
        jobs = self.backend.job_info(self.ids, self.filters)
        return ', '.join(str(i['id']) for i in jobs)


@register_action('info', 'show job infos')
class InfoAction(ReadActionBase):
    def format(self, args, tqdm_disable=False):
        info = self.backend.full_info(
            self.ids, self.filters, tqdm_disable=tqdm_disable)
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
        info = self.backend.full_info(
            self.ids, self.filters, tqdm_disable=tqdm_disable)
        outputs = []
        for i in info:
            if args.no_job_ids:
                outputs.append(i['command'])
            else:
                outputs.append(f"{i['id']}: {i['command']}")
        return '\n'.join(outputs)


@register_action(
    'export', 'show job infos in machine-readable format', aliases=['e'])
class ExportAction(ReadActionBase):
    export_options = {
        ('-e', '--export-format'): {
            'choices': ['json', 'yaml', 'toml'],
            'default': 'json',
            'help': 'The format of the output.',
        },
        ('-t', '--tail'): {
            'type': int,
            'default': None,
            'help':
                'The tail lines of the output. '
                'If 0, all lines will be shown.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.export_options)

    def extra_func(self, i, args):
        i['time_run'] = i['time_run'].total_seconds()
        if args.tail is not None:
            i['output'] = self.backend.output(i, args.tail)

    def format(self, args, tqdm_disable=False):
        extra_func = functools.partial(self.extra_func, args=args)
        info = self.backend.full_info(
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
        ('-i', '--interactive'): {
            'action': 'store_true',
            'help': 'Follow the output.',
        },
        ('-t', '--tail'): {
            'type': int,
            'default': 0,
            'help':
                'Number of lines. '
                'If 0, all lines will be shown.',
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
        info = self.backend.job_info(self.ids, self.filters)
        if not info:
            return 'No jobs found.'
        outputs = []
        for i in info:
            outputs.append(f'Job {i["id"]}:')
            out = self.backend.output(i, args.tail, shell=False)
            if not args.raw:
                out = textwrap.indent(out, '> ') + '\n'
            outputs.append(out)
        return '\n'.join(outputs).rstrip()

    def main(self, args):
        if not args.interactive:
            return super().main(args)
        info = self.backend.job_info(self.ids, self.filters)
        if not info:
            print('No jobs found.')
            return 1
        if len(info) > 1:
            print('Cannot follow multiple outputs.')
            return 1
        try:
            self.backend.output(info[0], args.tail, shell=True)
        except KeyboardInterrupt:
            pass
        return 0


@register_action('wait', 'Wait for jobs to finish', aliases=['w'])
class WaitAction(ReadActionBase):
    wait_options = {
        ('-p', '--progress'): {
            'action': 'store_true',
            'help': 'Show progress bar.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.wait_options)

    def main(self, args, tqdm_disable=False):
        f = FilterArgs(running=True, queued=True)
        info = self.backend.job_info(self.ids, f)
        pbar = tqdm(total=len(info)) if args.progress else None
        while True:
            remaining = self.backend.job_info(self.ids, f)
            if pbar:
                pbar.n = len(info) - len(remaining)
                pbar.refresh()
            time.sleep(1)
            if not remaining:
                break
        if pbar:
            pbar.close()
            print('')
        return 0

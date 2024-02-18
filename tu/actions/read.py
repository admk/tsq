import textwrap
import functools

from ..utils import timedelta_format
from ..wrapper import job_info, full_info, output
from .base import register_action
from .filter import FilterActionBase


@register_action('ls', 'show job infos in compact format')
class ListAction(FilterActionBase):
    options = {
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
        }
    }
    def add_arguments(self, parser):
        super().add_arguments(parser)
        for option, kwargs in self.options.items():
            parser.add_argument(*option, **kwargs)

    @staticmethod
    def shorten(text, max_len):
        return textwrap.shorten(text, width=max_len, placeholder='...')

    def main(self, args):
        import tabulate
        info = full_info(self.ids, self.filters)
        if not info:
            print('No jobs found.')
            return
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
        print(table)


@register_action('ids', 'show job IDs')
class IdsAction(FilterActionBase):
    def main(self, args):
        jobs = job_info(self.ids, self.filters)
        for i in jobs:
            print(i)


@register_action('info', 'show job infos')
class InfoAction(FilterActionBase):
    def main(self, args):
        info = full_info(self.ids, self.filters)
        if not info:
            print('No jobs found.')
            return
        for i in info:
            print(f'Job {i["id"]}:')
            for k, v in i.items():
                if k == 'id':
                    continue
                print(f'  {k}: {v}')


@register_action('commands', 'show job commands')
class CommandsAction(FilterActionBase):
    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '-n', '--no-job-ids', action='store_true',
            help='Do not print the job ID before the command.')

    def main(self, args):
        info = full_info(self.ids, self.filters)
        for i in info:
            if args.no_job_ids:
                print(i['command'])
            else:
                print(f"{i['id']}: {i['command']}")


@register_action('export', 'show job infos in machine-readable format')
class ExportAction(FilterActionBase):
    options = {
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
    def add_arguments(self, parser):
        super().add_arguments(parser)
        for option, kwargs in self.options.items():
            parser.add_argument(*option, **kwargs)

    def extra_func(self, i, args):
        i['time_run'] = i['time_run'].total_seconds()
        if args.tail:
            i['output'] = output(i['id'], tail=True)

    def main(self, args):
        extra_func = functools.partial(self.extra_func, args=args)
        info = full_info(self.ids, self.filters, extra_func=extra_func)
        if args.export_format == 'json':
            import json
            print(json.dumps(info, indent=4, default=str))
        elif args.export_format == 'yaml':
            import yaml
            print(yaml.dump(info, default_flow_style=False))
        elif args.export_format == 'toml':
            import toml
            print(toml.dumps({'job': info}))
        else:
            raise ValueError(f'Unknown format: {args.export_format}')


@register_action('outputs', 'Show job outputs')
class OutputsAction(FilterActionBase):
    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '-n', '--lines', type=int, default=10, help='Number of lines.')
        parser.add_argument(
            '-R', '--raw', action='store_true', help='Do not format lines.')
        parser.add_argument(
            '-j', '--json', action='store_true', help='Export in JSON format.')

    def main(self, args):
        info = job_info(self.ids, self.filters)
        for i in info:
            print(f'Job {i["id"]}:')
            out = output(i['id'])
            if not args.raw:
                out = '\n'.join(textwrap.wrap(out, width=80))
                out = textwrap.indent(out, '| ')
            print(out)
            print()

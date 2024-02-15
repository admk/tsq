import textwrap

import tabulate

from ..wrapper import job_info, full_info, output
from .base import register_action
from .filter import FilterActionBase


@register_action('ls', 'show job infos in compact format')
class ListAction(FilterActionBase):
    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '-l', '--length', type=int, default=30,
            help='Max length for each column.')

    @staticmethod
    def shorten(text, max_len):
        return textwrap.shorten(text, width=max_len, placeholder='...')

    def main(self, args):
        info = full_info(self.ids, self.filters)
        if not info:
            print('No jobs found.')
            return
        rows = []
        for i in info:
            rows.append({
                'id': i['id'],
                'status': i['status'],
                'gpus': i.get('gpus_required', 0),
                'slots': i.get('slots_required', 0),
                'time': i.get('time_run', ''),
                'command': self.shorten(i['command'], args.length),
            })
        table = tabulate.tabulate(
            rows, headers='keys', tablefmt='rounded_outline')
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
                print(f'{i["id"]}: {i["command"]}')


@register_action('export', 'show job infos in machine-readable format')
class ExportAction(FilterActionBase):
    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '-e', '--export-format', choices=['json', 'yaml', 'toml'],
            default='json', help='The format of the output.')
        parser.add_argument(
            '-t', '--tail', action='store_true',
            help='"tail -n 10 -f" the output of the job.')

    def main(self, args):
        info = full_info(self.ids, self.filters)
        if args.tail:
            for i in info:
                i['output'] = output(i['id'], tail=True)
        if args.export_format == 'json':
            import json
            print(json.dumps(info, indent=4, default=str))
        elif args.export_format == 'yaml':
            import yaml
            print(yaml.dump(info, default_flow_style=False))
        elif args.export_format == 'toml':
            import toml
            print(toml.dumps({'jobs': info}))


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

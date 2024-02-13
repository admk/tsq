import tabulate

from ..wrapper import job_info, full_info
from .base import register_action
from .filter import FilterActionBase


@register_action('ls', 'show job infos in compact format')
class DefaultAction(FilterActionBase):
    def main(self, args):
        info = full_info(self.ids, self.filters)
        if not info:
            print('No jobs found.')
            return
        info = [
            {
                'id': j,
                'status': i['status'],
                'gpus': i.get('gpus_required', 0),
                'slots': i.get('slots_required', 0),
                'time': i.get('time_run', ''),
                'command': i['command'],
            }
            for j, i in info.items()]
        table = tabulate.tabulate(
            info, headers='keys', tablefmt='rounded_outline', maxcolwidths=20)
        print(tabulate.__version__)
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
        for j, i in info.items():
            print(f'Job {j}:')
            for k, v in i.items():
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
        for j, i in info.items():
            if args.no_job_ids:
                print(i['command'])
            else:
                print(f'{j}: {i["command"]}')


@register_action('export', 'show job infos in machine-readable format')
class ExportAction(FilterActionBase):
    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '-e', '--export-format', choices=['json', 'yaml', 'toml'],
            default='json', help='The format of the output.')

    def main(self, args):
        info = full_info(self.ids, self.filters)
        if args.export_format == 'json':
            import json
            print(json.dumps(info, indent=4, default=str))
        elif args.export_format == 'yaml':
            import yaml
            print(yaml.dump(info, default_flow_style=False))
        elif args.export_format == 'toml':
            import toml
            print(toml.dumps({str(k): v for k, v in info.items()}))

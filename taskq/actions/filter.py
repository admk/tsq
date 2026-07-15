import sys

from ..common import STATUSES, FilterArgs
from .base import ActionBase, CLIError


def parse_id_selector(value):
    ids = []
    for item in value.split(','):
        item = item.strip()
        if not item:
            raise CLIError(
                f'invalid job ID selector {value!r}; expected IDs or ranges '
                'like "1-3,5"')
        if '-' in item:
            parts = item.split('-')
            if len(parts) != 2:
                raise CLIError(
                    f'invalid job ID range {item!r}; expected "start-end"')
            start, end = parts
            if not start.isdigit() or not end.isdigit():
                raise CLIError(
                    f'invalid job ID range {item!r}; expected numeric IDs')
            start_id = int(start)
            end_id = int(end)
            if start_id > end_id:
                raise CLIError(
                    f'invalid job ID range {item!r}; start is greater than end')
            ids += list(range(start_id, end_id + 1))
        else:
            if not item.isdigit():
                raise CLIError(
                    f'invalid job ID {item!r}; expected a numeric ID')
            ids.append(int(item))
    return ids


class FilterActionBase(ActionBase):
    filter_options = {
        ('id', ): {
            'type': str,
            'default': None,
            'nargs': '?',
            'help':
                'Optional ranges of job IDs to perform the action on, '
                'a comma-separated list of ranges, e.g. "1-3,5,7-9". '
                'If not provided, all jobs will be affected. '
                'If only "-" is specified, it reads from stdin.'
        },
        ('-A', '--all'): {
            'action': 'store_true',
            'help': 'Perform the action on all jobs.',
        },
        ('-r', '--running'): {
            'action': 'store_true',
            'help': 'Perform the action on running jobs.',
        },
        ('-q', '--queued'): {
            'action': 'store_true',
            'help': 'Perform the action on queued/allocating jobs.',
        },
        ('--merging', ): {
            'action': 'store_true',
            'help': 'Perform the action on jobs waiting to merge.',
        },
        ('-s', '--success'): {
            'action': 'store_true',
            'help': 'Perform the action on successful jobs.',
        },
        ('-f', '--failed'): {
            'action': 'store_true',
            'help': 'Perform the action on failed jobs.',
        },
        ('-k', '--killed'): {
            'action': 'store_true',
            'help': 'Perform the action on killed jobs.',
        },
        ('-i', '--interrupted'): {
            'action': 'store_true',
            'help': 'Perform the action on interrupted jobs.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.filter_options)

    def _parse_ids(self, args):
        if args.id == '-':
            args.id = sys.stdin.read().strip()
        if not args.id:
            self.ids = None
            return
        self.ids = parse_id_selector(args.id)

    def _parse_filters(self, args):
        self.filters = FilterArgs(
            force_all=args.all,
            running=args.running,
            queued=args.queued,
            merging=args.merging,
            success=args.success,
            failed=args.failed,
            killed=args.killed,
            interrupted=args.interrupted,
        )

    def transform_args(self, args):
        self._parse_ids(args)
        self._parse_filters(args)
        return args

    @property
    def has_filters(self):
        filters = [self.ids, self.filters.force_all]
        filters += [getattr(self.filters, a) for a in STATUSES]
        return any(filters)

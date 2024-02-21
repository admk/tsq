import sys

from ..common import STATUSES, FilterArgs
from .base import ActionBase


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
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.filter_options)

    def _parse_ids(self, args):
        if args.id == '-':
            args.id = sys.stdin.read().strip()
            return
        if not args.id:
            self.ids = None
            return
        ids = []
        for i in args.id.split(','):
            if '-' in i:
                start, end = i.split('-')
                ids += list(range(int(start), int(end) + 1))
            else:
                ids.append(int(i))
        self.ids = ids

    def _parse_filters(self, args):
        self.filters = FilterArgs(
            force_all=args.all,
            running=args.running,
            queued=args.queued,
            success=args.success,
            failed=args.failed,
            killed=args.killed,
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

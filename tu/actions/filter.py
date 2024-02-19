from dataclasses import dataclass

from ..common import STATUSES
from .base import ActionBase


@dataclass
class FilterArgs:
    force_all: bool = False
    running: bool = False
    allocating: bool = False
    success: bool = False
    failed: bool = False
    killed: bool = False

    @property
    def all(self):
        flags = all(not getattr(self, a) for a in STATUSES)
        return self.force_all or flags


class FilterActionBase(ActionBase):
    filter_options = {
        ('-i', '--id'): {
            'type': str,
            'default': None,
            'help':
                'The ranges of job IDs to perform the action on, '
                'a comma-separated list of ranges, e.g. "1-3,5,7-9". '
                'If not provided, all jobs will be affected.',
        },
        ('-A', '--all'): {
            'action': 'store_true',
            'help': 'Perform the action on all jobs.',
        },
        ('-r', '--running'): {
            'action': 'store_true',
            'help': 'Perform the action on running jobs.',
        },
        ('-a', '--allocating'): {
            'action': 'store_true',
            'help': 'Perform the action on allocating jobs.',
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
            allocating=args.allocating,
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

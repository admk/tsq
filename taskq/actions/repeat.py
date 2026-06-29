import re
import shlex
from dataclasses import dataclass, field

from .. import TOOL_NAME
from ..common import tqdm
from ..utils import escape_command_display
from .base import CLIError


repeat_options = {
    ('-R', '--repeat'): {
        'type': int,
        'default': 1,
        'metavar': 'COUNT',
        'help': (
            'Add each selected command COUNT times. Repeated instances of '
            'the same command are chained with dependencies.'),
    },
    ('--no-chain', ): {
        'action': 'store_false',
        'dest': 'chain',
        'default': True,
        'help': 'Do not add dependencies between repeated instances.',
    },
}


@dataclass
class AddRequest:
    command: str
    gpus: object
    slots: object
    depends_on: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)


def validate_repeat_count(count):
    if count < 1:
        raise CLIError('repeat count must be at least 1')
    return count


def include_gpus(gpus):
    try:
        return int(gpus) > 0
    except (TypeError, ValueError):
        return bool(gpus)


def dry_command_argv(command):
    argv = shlex.split(command)
    if len(argv) == 1 and re.search(r'\s', argv[0]):
        try:
            split_arg = shlex.split(argv[0])
        except ValueError:
            return argv
        if len(split_arg) > 1:
            return split_arg
    return argv


def dry_add_command(command, gpus, slots, depends_on=None, ref=None):
    argv = [TOOL_NAME, 'add']
    if ref:
        argv += ['--ref', str(ref)]
    if include_gpus(gpus):
        argv += ['-G', str(gpus)]
    argv += ['-N', str(slots)]
    if depends_on:
        argv += ['-D', ','.join(str(i) for i in depends_on)]
    argv += dry_command_argv(command)
    return escape_command_display(shlex.join(argv))


def unique_append(values, value):
    if value not in values:
        values.append(value)


def add_repeated(
    backend, requests, repeat, commit=True, dry_run=None, desc='add',
    chain=True,
):
    id_groups = []
    previous_by_command = {}
    for request in tqdm(requests, desc=desc):
        ids = []
        for _ in range(repeat):
            depends_on = list(request.depends_on or [])
            previous_id = previous_by_command.get(request.command)
            if chain and previous_id is not None:
                unique_append(depends_on, previous_id)
            if not commit:
                if dry_run:
                    dry_run(request, depends_on)
                job_id = '<id>'
            else:
                job_id = backend.add(
                    request.command,
                    request.gpus,
                    request.slots,
                    depends_on=depends_on,
                    **request.kwargs,
                )
            ids.append(job_id)
            previous_by_command[request.command] = job_id
        id_groups.append(ids)
    return id_groups

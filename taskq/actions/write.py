import sys

from .. import TOOL_NAME
from ..common import tqdm
from .base import register_action, DryActionBase
from .filter import FilterActionBase
from .repeat import (
    AddRequest,
    add_repeated,
    dry_add_command,
    repeat_options,
    validate_repeat_count,
)



class WriteActionBase(FilterActionBase, DryActionBase):
    def transform_args(self, args):
        args = DryActionBase.transform_args(self, args)
        args = FilterActionBase.transform_args(self, args)
        if not self.has_filters:
            print(
                f'{TOOL_NAME}: flags or ids must be specified '
                f'for dangerous action {self.name!r}.')
            sys.exit(1)
        return args


@register_action('kill', 'kill jobs', aliases=['k'])
class KillAction(WriteActionBase):
    def kill(self, info, commit):
        for i in tqdm(info, desc='kill'):
            self.backend.kill(i, commit=commit)

    def main(self, args):
        info = self.backend.job_info(self.ids, self.filters)
        info = [i for i in info if i['status'] == 'running']
        if not info:
            print('No job to kill.')
            return
        self.kill(info, args.commit)
        killed_ids = ', '.join(str(i['id']) for i in info)
        print('Killed:', killed_ids)


@register_action('remove', 'remove jobs', aliases=['rm'])
class RemoveAction(WriteActionBase):
    def remove(self, info, commit):
        queued = [i for i in info if i['status'] == 'queued']
        remaining = [i for i in info if i['status'] != 'queued']
        for i in tqdm(queued + remaining, desc='remove'):
            self.backend.remove(i, commit=commit)

    def main(self, args):
        info = self.backend.job_info(self.ids, self.filters)
        if not info:
            print('No job to remove.')
            return
        self.remove(info, args.commit)
        removed_ids = ', '.join(str(i['id']) for i in info)
        print('Removed:', removed_ids)


@register_action('rerun', 'rerun jobs', aliases=['rr'])
class RerunAction(WriteActionBase):
    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(repeat_options)

    def transform_args(self, args):
        args = super().transform_args(args)
        args.repeat = validate_repeat_count(args.repeat)
        return args

    @staticmethod
    def format_id_chain(ids):
        return ' -> '.join(str(job_id) for job_id in ids)

    def rerun(self, info, commit, repeat=1):
        requests = []
        for i in info:
            command = i['command']
            gpus = i['gpus_required']
            slots = i['slots_required']
            kwargs = {}
            if commit and hasattr(self.backend, 'rerun_env'):
                kwargs['env'] = self.backend.rerun_env(i)
            if i.get('git_commit'):
                kwargs.update({
                    'git_ref': i.get('git_ref') or i['git_commit'],
                    'git_commit': i['git_commit'],
                    'git_root': i.get('git_root'),
                    'source_cwd': i.get('source_cwd') or i.get('cwd'),
                })
            requests.append(AddRequest(command, gpus, slots, kwargs=kwargs))
        return add_repeated(
            self.backend, requests, repeat, commit=commit,
            dry_run=lambda request, depends_on: print(
                dry_add_command(
                    request.command,
                    request.gpus,
                    request.slots,
                    depends_on,
                    request.kwargs.get('git_ref'),
                )
            ),
            desc='rerun',
        )

    def main(self, args):
        info = self.backend.full_info(self.ids, self.filters)
        if not info:
            print('No job to rerun.')
            return
        new_ids = self.rerun(info, args.commit, args.repeat)
        reran_ids = ', '.join(
            f"{i['id']} -> {self.format_id_chain(j)}"
            for i, j in zip(info, new_ids))
        print('Reran:', reran_ids)


@register_action('requeue', 'requeue jobs', aliases=['rq'])
class RequeueAction(RerunAction, RemoveAction):
    def main(self, args):
        info = self.backend.full_info(self.ids, self.filters)
        if not info:
            print('No job to requeue.')
            return
        new_ids = self.rerun(info, args.commit, args.repeat)
        self.remove(info, args.commit)
        requeued_ids = ', '.join(
            f"{i['id']} -> {self.format_id_chain(j)}"
            for i, j in zip(info, new_ids))
        print('Requeued:', requeued_ids)

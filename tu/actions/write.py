import sys

from ..wrapper import tqdm, job_info, full_info, add, remove
from .base import register_action, DryActionBase
from .filter import FilterActionBase



class WriteActionBase(FilterActionBase, DryActionBase):
    def add_arguments(self, parser):
        DryActionBase.add_arguments(self, parser)
        FilterActionBase.add_arguments(self, parser)

    def transform_args(self, args):
        args = DryActionBase.transform_args(self, args)
        args = FilterActionBase.transform_args(self, args)
        if not self.has_filters:
            print(f'tsu: flags or ids must be specified for {self.name}.')
            sys.exit(1)
        return args


@register_action('remove', 'remove jobs')
class RemoveAction(WriteActionBase):
    def remove(self, ids, commit):
        iterer = tqdm(ids, commit=commit)
        for i in iterer:
            iterer.set_description(f'Removing {i}')
            remove(i, commit=commit)
        return ids

    def main(self, args):
        info = job_info(self.ids, self.filters)
        ids = [i['id'] for i in info]
        removed_ids = self.remove(ids, args.commit)
        print('Removed:', ', '.join(str(i) for i in removed_ids))


@register_action('rerun', 'rerun jobs')
class RerunAction(WriteActionBase):
    def rerun(self, info, commit):
        iterer = tqdm(info, commit=commit)
        reran_ids = []
        for i in iterer:
            reran_ids += [i['id']]
            iterer.set_description(f'Requeueing {i["id"]}')
            command = i['command']
            gpus = i['gpus_required']
            slots = i['slots_required']
            add(command, gpus, slots, commit=commit)
        return reran_ids

    def main(self, args):
        info = full_info(self.ids, self.filters)
        reran_ids = self.rerun(info, args.commit)
        print('Reran:', ', '.join(str(i) for i in reran_ids))


@register_action('requeue', 'requeue jobs')
class RequeueAction(RerunAction, RemoveAction):
    def main(self, args):
        info = full_info(self.ids, self.filters)
        reran_ids = self.rerun(info, args.commit)
        self.remove(reran_ids, args.commit)
        print('Requeued:', ', '.join(str(i) for i in reran_ids))

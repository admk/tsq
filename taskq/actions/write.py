import sys

from ..common import tqdm
from .base import register_action, DryActionBase
from .filter import FilterActionBase



class WriteActionBase(FilterActionBase, DryActionBase):
    def transform_args(self, args):
        args = DryActionBase.transform_args(self, args)
        args = FilterActionBase.transform_args(self, args)
        if not self.has_filters:
            print(
                'tsu: flags or ids must be specified '
                f'for dangerous action {self.name!r}.')
            sys.exit(1)
        return args


@register_action('kill', 'kill jobs', aliases=['k'])
class KillAction(WriteActionBase):
    def kill(self, info, commit):
        for i in tqdm(info):
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
        for i in tqdm(queued + remaining):
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
    def rerun(self, info, commit):
        new_ids = []
        for i in tqdm(info):
            command = i['command']
            gpus = i['gpus_required']
            slots = i['slots_required']
            ji = self.backend.add(command, gpus, slots, commit=commit)
            new_ids.append(ji)
        return new_ids

    def main(self, args):
        info = self.backend.full_info(self.ids, self.filters)
        if not info:
            print('No job to rerun.')
            return
        new_ids = self.rerun(info, args.commit)
        reran_ids = ', '.join(
            f"{i['id']} -> {j}" for i, j in zip(info, new_ids))
        print('Reran:', reran_ids)


@register_action('requeue', 'requeue jobs', aliases=['rq'])
class RequeueAction(RerunAction, RemoveAction):
    def main(self, args):
        info = self.backend.full_info(self.ids, self.filters)
        if not info:
            print('No job to requeue.')
            return
        new_ids = self.rerun(info, args.commit)
        self.remove(info, args.commit)
        requeued_ids = ', '.join(
            f"{i['id']} -> {j}" for i, j in zip(info, new_ids))
        print('Requeued:', requeued_ids)

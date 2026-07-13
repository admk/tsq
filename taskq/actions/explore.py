import json
from pathlib import Path

from .base import ActionBase, CLIError, register_action
from ..explore.git import ensure_local_exclude, repository_root
from ..explore.profiles import ExploreProfileStore
from ..explore.wizard import (
    ExploreInitWizard,
    WizardAbort,
    choose_profile,
    confirm_remove,
    interactive,
)
from ..explore.workflow import ExploreWorkflow


@register_action('explore', 'run autonomous optimization campaigns')
class ExploreAction(ActionBase):
    options = {
        ('explore_action',): {
            'nargs': '?',
            'choices': [
                'init', 'start', 'remove', 'status', 'inspect',
                'pause', 'resume', 'stop',
            ],
            'help': 'Campaign action; omit to list campaigns.',
        },
        ('value',): {
            'nargs': '?',
            'help': 'Profile name for init/start/remove, otherwise a campaign ID.',
        },
        ('attempt',): {
            'nargs': '?',
            'help': 'Attempt ID for inspect.',
        },
        ('--yes',): {'action': 'store_true'},
        ('--cmd',): {'dest': 'command', 'default': None},
        ('--check',): {'action': 'append', 'dest': 'checks', 'default': None},
        ('--score',): {'default': None},
        ('--score-direction',): {'choices': ['min', 'max'], 'default': None},
        ('--min-improvement',): {'type': float, 'default': None},
        ('--protect',): {'action': 'append', 'default': None},
        ('--parallel',): {'type': int, 'default': None},
        ('--max-adjustments',): {'type': int, 'default': None},
        ('--max-accepted-attempts',): {'type': int, 'default': None},
        ('--max-time',): {'default': None},
        ('--max-files',): {'type': int, 'default': None},
        ('--max-lines',): {'type': int, 'default': None},
        ('--json',): {'action': 'store_true', 'dest': 'as_json'},
    }

    def __init__(self, name, parser_kwargs):
        ActionBase.__init__(self, name, parser_kwargs)
        self.options.update(type(self).options)

    @staticmethod
    def _campaign_line(campaign):
        return '{id}  {status}  {objective}'.format(**campaign)

    def main(self, args):
        action = args.explore_action
        root = store = None
        if action in {'init', 'start', 'remove'}:
            root = repository_root(Path.cwd())
            ensure_local_exclude(root)
            store = ExploreProfileStore(root, self.backend.config)
        if action == 'init':
            if not interactive():
                raise CLIError('tq explore init requires an interactive terminal')
            name = self._profile_name(store, args.value)
            if name is None:
                return 130
            profile = store.create(name)
            result = ExploreInitWizard(store, profile).run(restart_complete=True)
            if not result:
                print('Initialized exploration profile {}.'.format(name))
                print('Start with: tq explore start {}'.format(name))
            return result
        if action == 'remove':
            if not args.value:
                raise CLIError('tq explore remove requires a profile name')
            profile = store.load(args.value)
            workflow = ExploreWorkflow(self.backend, self.backend.config)
            campaigns = workflow.profile_campaigns(root, profile.name)
            active = [
                campaign['id'] for campaign in campaigns
                if campaign['status'] not in {'completed', 'failed'}]
            if active:
                raise CLIError(
                    'profile has active campaigns: {}'.format(', '.join(active)))
            if not args.yes:
                if not interactive():
                    raise CLIError('non-interactive removal requires --yes')
                summary = (
                    '{}\n{} finished campaign(s) and their stored memory will '
                    'also be removed.'.format(profile.path, len(campaigns)))
                try:
                    approved = confirm_remove(profile.name, summary)
                except WizardAbort:
                    return 130
                if not approved:
                    print('Removal cancelled.')
                    return 0
            removed = workflow.remove_finished_profile_campaigns(
                root, profile.name)
            store.remove(profile.name)
            print('Removed profile {} and {} finished campaign(s).'.format(
                profile.name, len(removed)))
            return 0
        workflow = ExploreWorkflow(self.backend, self.backend.config)
        if action is None:
            campaigns = workflow.list()
            if args.as_json:
                print(json.dumps(campaigns, indent=2, sort_keys=True))
            elif not campaigns:
                print('No exploration campaigns found.')
            else:
                print('\n'.join(self._campaign_line(item) for item in campaigns))
            return
        if action == 'start':
            name = self._profile_name(store, args.value)
            if name is None:
                return 130
            profile = store.create(name)
            if not profile.complete:
                if not interactive():
                    raise CLIError(
                        'profile is incomplete; resume with: tq explore init {}'.format(
                            name))
                result = ExploreInitWizard(store, profile).run(
                    restart_complete=False)
                if result:
                    return result
            profile = store.load(name)
            config = store.effective_config(profile)
            campaign = workflow.start(
                profile.objective, profile_name=name, profile_config=config,
                command=args.command,
                checks=args.checks, score=args.score,
                score_direction=args.score_direction,
                min_improvement=args.min_improvement,
                protect=args.protect, parallel=args.parallel,
                max_adjustments=args.max_adjustments,
                max_accepted_attempts=args.max_accepted_attempts,
                max_time=args.max_time,
                max_files=args.max_files, max_lines=args.max_lines,
            )
            print('Started exploration {} on {}.'.format(
                campaign['id'], campaign['target_ref']))
            return
        if action == 'status':
            result = workflow.status(args.value)
            if args.as_json:
                print(json.dumps(result, indent=2, sort_keys=True))
                return
            campaign = result['campaign']
            print(self._campaign_line(campaign))
            print('generation: {}  attempts: {}  merge queue: {}'.format(
                campaign['generation'], result['counts']['attempts'],
                result['counts'].get('merge_requests_queued', 0) +
                result['counts'].get('merge_requests_processing', 0)))
            if result.get('manual_landing'):
                landing = result['manual_landing']
                print('warning: {}'.format(landing['message']))
                print('reason: {}'.format(landing['reason']))
                print('run: {}'.format(landing['command']))
            for attempt in result['attempts']:
                print('  {}  {}  {}'.format(
                    attempt['id'], attempt['status'], attempt['branch']))
            return
        if action == 'inspect':
            result = workflow.inspect(args.value, args.attempt)
            if args.as_json:
                print(json.dumps(result, indent=2, sort_keys=True))
                return
            for attempt in result['attempts']:
                print('{}  {}\nworktree: {}\n{}'.format(
                    attempt['id'], attempt['status'], attempt['worktree'],
                    attempt['diff'] or 'No diff.'))
            return
        target = {'pause': 'paused', 'resume': 'active', 'stop': 'draining'}[action]
        campaign = workflow.set_status(args.value, target)
        print('{}: {}'.format(campaign['id'], campaign['status']))

    @staticmethod
    def _profile_name(store, value):
        if value:
            return store.validate_name(value)
        if not interactive():
            raise CLIError('an exploration profile name is required outside a TTY')
        try:
            return store.validate_name(choose_profile(store))
        except WizardAbort:
            return None

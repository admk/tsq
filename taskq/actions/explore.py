import json

from .base import ActionBase, CLIError, register_action
from ..explore.workflow import ExploreWorkflow


@register_action('explore', 'run autonomous optimization campaigns')
class ExploreAction(ActionBase):
    options = {
        ('explore_action',): {
            'nargs': '?',
            'choices': ['start', 'status', 'inspect', 'pause', 'resume', 'stop'],
            'help': 'Campaign action; omit to list campaigns.',
        },
        ('value',): {
            'nargs': '?',
            'help': 'Objective for start, otherwise a campaign ID.',
        },
        ('attempt',): {
            'nargs': '?',
            'help': 'Attempt ID for inspect.',
        },
        ('--name',): {'default': None},
        ('--cmd',): {'dest': 'command', 'default': None},
        ('--check',): {'action': 'append', 'dest': 'checks', 'default': None},
        ('--score',): {'default': None},
        ('--score-direction',): {'choices': ['min', 'max'], 'default': None},
        ('--min-improvement',): {'type': float, 'default': None},
        ('--protect',): {'action': 'append', 'default': None},
        ('--parallel',): {'type': int, 'default': None},
        ('--max-adjustments',): {'type': int, 'default': None},
        ('--max-agent-jobs',): {'type': int, 'default': None},
        ('--max-merges',): {'type': int, 'default': None},
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
        workflow = ExploreWorkflow(self.backend, self.backend.config)
        action = args.explore_action
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
            if not args.value:
                raise CLIError('tq explore start requires an objective')
            campaign = workflow.start(
                args.value, name=args.name, command=args.command,
                checks=args.checks, score=args.score,
                score_direction=args.score_direction,
                min_improvement=args.min_improvement,
                protect=args.protect, parallel=args.parallel,
                max_adjustments=args.max_adjustments,
                max_agent_jobs=args.max_agent_jobs,
                max_merges=args.max_merges, max_time=args.max_time,
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

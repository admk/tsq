import json
import re
import shlex
import shutil
import sys
import time
from pathlib import Path

from ..backends import git_ref as git_ref_utils
from ..backends.base import BackendError
from .agent import parse_command_template
from .controller import ExploreController
from .git import campaign_id, diff, ensure_local_exclude, repository, require_clean
from .state import ExploreState


def _plain(value):
    return json.loads(json.dumps(value))


def _duration(value):
    if isinstance(value, (int, float)):
        return float(value)
    match = re.fullmatch(r'\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*', str(value))
    if not match:
        raise ValueError('invalid duration {!r}'.format(value))
    scale = {'': 1, 's': 1, 'm': 60, 'h': 3600, 'd': 86400}
    return float(match.group(1)) * scale[match.group(2)]


def _public_campaign(campaign):
    campaign = dict(campaign)
    config = dict(campaign.get('config') or {})
    config.pop('backend_config', None)
    campaign['config'] = config
    return campaign


def _option(overrides, config, name, default=None, config_name=None):
    value = overrides.get(name)
    if value is not None:
        return value
    return config.get(config_name or name, default)


class ExploreWorkflow:
    def __init__(self, backend, config):
        self.backend = backend
        self.config = config
        if backend.name != 'tmux':
            raise BackendError('tq explore is supported only by the tmux backend')

    def _state_path(self, root):
        return Path(root) / '.tq' / 'explore' / 'state.sqlite'

    def _open(self, root):
        path = self._state_path(root)
        if not path.is_file():
            raise BackendError('no exploration campaigns found')
        return ExploreState(path)

    @staticmethod
    def _latest(state, campaign=None):
        if campaign:
            value = state.get_campaign(campaign)
            if value is None:
                raise BackendError('exploration campaign not found: {}'.format(campaign))
            return value
        values = state.list_campaigns(limit=1)
        if not values:
            raise BackendError('no exploration campaigns found')
        return values[0]

    def start(self, objective, **overrides):
        if not objective or not objective.strip():
            raise BackendError('exploration objective cannot be empty')
        root, target_ref, target_head = repository(Path.cwd())
        require_clean(root)
        ensure_local_exclude(root)
        explore = _plain(self.config.get('explore', {}))
        try:
            command = parse_command_template(
                _option(overrides, explore, 'command'))
        except (TypeError, ValueError) as error:
            raise BackendError(str(error)) from error
        score = _option(overrides, explore, 'score')
        score_direction = _option(overrides, explore, 'score_direction')
        if score and score_direction not in {'min', 'max'}:
            raise BackendError('--score-direction min|max is required with --score')
        identifier = campaign_id(overrides.get('name') or objective)
        work_root = Path(self.backend.state_dir) / 'explore' / identifier
        mainline_worktree = work_root / 'mainline'
        control_cwd = work_root / 'control'
        control_cwd.mkdir(parents=True, exist_ok=True)
        mainline_branch = 'tq/explore/{}/mainline'.format(identifier)
        heartbeat_file = work_root / 'heartbeat'
        state_path = self._state_path(root)
        state = ExploreState(state_path)
        if state.get_campaign(identifier):
            raise BackendError('exploration campaign already exists: {}'.format(identifier))
        workspace = None
        campaign_created = False
        controller_registered = False
        try:
            workspace = git_ref_utils.create_branch_worktree(
                root, mainline_branch, mainline_worktree, target_head)
            try:
                parallel = int(_option(overrides, explore, 'parallel', 4))
                max_adjustments = int(
                    _option(overrides, explore, 'max_adjustments', 3))
                max_agent_jobs = int(
                    _option(overrides, explore, 'max_agent_jobs', 32))
                max_merges = int(_option(overrides, explore, 'max_merges', 6))
                max_time = _duration(_option(
                    overrides, explore, 'max_time', 28800, 'max_wall_time'))
                max_files = int(_option(overrides, explore, 'max_files', 5))
                max_lines = int(_option(overrides, explore, 'max_lines', 300))
                min_improvement = float(_option(
                    overrides, explore, 'min_improvement', 0))
                controller_interval = float(explore.get('controller_interval', 5))
                controller_timeout = float(explore.get('controller_timeout', 30))
                action_timeout = float(explore.get('action_timeout', 1800))
            except (TypeError, ValueError) as error:
                raise BackendError(
                    'invalid exploration setting: {}'.format(error)) from error
            if min(
                parallel, max_adjustments, max_agent_jobs, max_merges,
                max_time, max_files, max_lines, controller_interval,
                controller_timeout,
                action_timeout,
            ) <= 0:
                raise BackendError('exploration limits must be positive')
            if min_improvement < 0:
                raise BackendError('minimum improvement cannot be negative')
            budgets = {
                'parallel': parallel,
                'max_adjustments': max_adjustments,
                'max_agent_jobs': max_agent_jobs,
                'max_merges': max_merges,
                'max_wall_time': max_time,
                'deadline': time.time() + max_time,
            }
            protected = list(explore.get('protected', []))
            protected.extend(overrides.get('protect') or [])
            checks = list(_option(overrides, explore, 'checks', []))
            for command_text in checks + ([score] if score else []):
                for token in shlex.split(command_text):
                    path = (Path(root) / token).resolve()
                    try:
                        relative = path.relative_to(Path(root))
                    except ValueError:
                        continue
                    if path.is_file():
                        protected.append(str(relative))
            campaign_config = {
                'repo_root': root,
                'work_root': str(work_root),
                'mainline_branch': mainline_branch,
                'mainline_worktree': str(mainline_worktree),
                'control_cwd': str(control_cwd),
                'heartbeat_file': str(heartbeat_file),
                'backend_config': _plain(self.backend.config),
                'command': command,
                'checks': checks,
                'score': score,
                'score_direction': score_direction,
                'min_improvement': min_improvement,
                'protected_paths': list(dict.fromkeys(protected)),
                'max_files': max_files,
                'max_lines': max_lines,
                'controller_interval': controller_interval,
                'controller_timeout': controller_timeout,
                'action_timeout': action_timeout,
                'workspace': workspace,
            }
            state.create_campaign(
                identifier, objective.strip(), target_ref, mainline_branch,
                target_head=target_head, budgets=budgets, config=campaign_config)
            campaign_created = True
            argv = [
                sys.executable, '-m', 'taskq.explore.controller',
                '--state', str(state_path), '--campaign', identifier,
            ]
            self.backend.register_controller(
                identifier, argv, root, heartbeat_file,
                campaign_config['controller_timeout'])
            controller_registered = True
            ExploreController(state, self.backend, identifier).reconcile()
            return state.get_campaign(identifier)
        except Exception as error:
            if controller_registered or campaign_created:
                try:
                    self.backend.unregister_controller(identifier)
                except Exception:
                    pass
            if campaign_created:
                for job in state.list_jobs(campaign_id=identifier):
                    try:
                        self.backend.kill({'id': int(job['backend_job_id'])})
                    except Exception:
                        pass
                state.update_campaign(
                    identifier, status='failed', finished_at=time.strftime(
                        '%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    config=dict(campaign_config, startup_error=str(error)))
            if workspace:
                git_ref_utils.remove_branch_worktree(
                    workspace, delete_branch=True, force_branch=True)
            git_ref_utils.remove_nested_worktrees(work_root)
            shutil.rmtree(work_root, ignore_errors=True)
            raise
        finally:
            state.close()

    def list(self, root=None):
        root = root or repository(Path.cwd())[0]
        path = self._state_path(root)
        if not path.exists():
            return []
        with ExploreState(path) as state:
            return [_public_campaign(item) for item in state.list_campaigns()]

    def status(self, campaign=None):
        root = repository(Path.cwd())[0]
        with self._open(root) as state:
            value = self._latest(state, campaign)
            return {
                'campaign': _public_campaign(value),
                'counts': state.counts(value['id']),
                'attempts': state.list_attempts(value['id']),
                'merge_requests': state.list_merge_requests(value['id']),
                'decisions': state.list_decisions(value['id'])[-5:],
                'findings': state.list_findings(value['id'], limit=5),
            }

    def inspect(self, campaign=None, attempt=None):
        root = repository(Path.cwd())[0]
        with self._open(root) as state:
            value = self._latest(state, campaign)
            attempts = state.list_attempts(value['id'])
            if attempt:
                attempts = [item for item in attempts if item['id'] == attempt]
                if not attempts:
                    raise BackendError('attempt not found: {}'.format(attempt))
            result = []
            for item in attempts:
                patch = ''
                if Path(item['worktree']).is_dir():
                    patch = diff(item['worktree'], item['base_head'], item['head'])
                if not patch:
                    reviews = state.list_jobs(
                        attempt_id=item['id'], role='reviewer')
                    if reviews:
                        patch = reviews[-1]['metadata'].get(
                            'artifacts', {}).get('diff', '')
                result.append(dict(item, diff=patch))
            return {'campaign': _public_campaign(value), 'attempts': result}

    def set_status(self, campaign, status):
        root = repository(Path.cwd())[0]
        with self._open(root) as state:
            value = self._latest(state, campaign)
            current = value['status']
            if current in {'completed', 'failed', 'landing_failed'}:
                raise BackendError(
                    'exploration campaign {} is {}'.format(value['id'], current))
            if status == current:
                return value
            config = dict(value['config'])
            if status == 'paused':
                config['paused_from'] = current
            elif status == 'active':
                if current != 'paused':
                    raise BackendError(
                        'exploration campaign {} is not paused'.format(value['id']))
                status = config.pop('paused_from', 'active')
            elif status == 'draining':
                config.pop('paused_from', None)
            else:
                raise BackendError('invalid exploration status: {}'.format(status))
            value = state.update_campaign(value['id'], status=status, config=config)
            if status != 'paused':
                self._register(state, value)
                ExploreController(state, self.backend, value['id']).reconcile()
            return value

    def _register(self, state, campaign):
        config = campaign['config']
        argv = [
            sys.executable, '-m', 'taskq.explore.controller',
            '--state', str(state.path), '--campaign', campaign['id'],
        ]
        self.backend.register_controller(
            campaign['id'], argv, config['repo_root'],
            config['heartbeat_file'], config['controller_timeout'])

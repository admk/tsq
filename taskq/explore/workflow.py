import json
import os
import re
import shlex
import shutil
import sys
import time
from pathlib import Path

from ..backends import git_ref as git_ref_utils
from ..backends.base import BackendError
from .agent import parse_command_template, render_prompt
from .controller import ExploreController
from .git import campaign_id, diff, ensure_local_exclude, repository, require_clean
from .state import ExploreState
from .assets import copy_assets, inventory
from .environment import EnvironmentConfigError, resolve_environment
from .profiles import resolve_phases


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
    if config.get('env'):
        config['env'] = {
            name: '<redacted>' for name in config['env']}
    campaign['config'] = config
    return campaign


def _required_text(config, name, phase):
    value = config.get(name)
    if not isinstance(value, str) or not value.strip():
        raise BackendError(
            'explore.{}.{} must be non-empty text'.format(phase, name))
    try:
        render_prompt(value, **{
            'objective': 'objective', 'direction': {}, 'memory': {},
            'tried_directions': [], 'artifacts': {}, 'context': {},
            'direction_count': 1, 'change_scope': 'unlimited',
            'original_prompt': 'prompt', 'error': 'error',
        })
    except ValueError as error:
        raise BackendError(
            'invalid explore.{}.{}: {}'.format(phase, name, error)) from error
    return value


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

    def start(self, objective, profile_name=None, profile_config=None):
        if not objective or not objective.strip():
            raise BackendError('exploration objective cannot be empty')
        root, target_ref, target_head = repository(Path.cwd())
        ensure_local_exclude(root)
        require_clean(root)
        source_config = profile_config or self.config
        explore = _plain(source_config.get('explore', {}))
        inherited_environment = dict(
            getattr(self.backend, 'env', {}) or {})
        inherited_environment.update(os.environ)
        try:
            campaign_environment = resolve_environment(
                explore.get('env', {}), root, inherited_environment)
        except EnvironmentConfigError as error:
            raise BackendError(str(error)) from error
        phases = resolve_phases(explore)
        try:
            commands = {
                name: parse_command_template(phases[name].get('command'))
                for name in ('planning', 'optimization', 'fix', 'merge')
            }
        except (TypeError, ValueError) as error:
            raise BackendError(str(error)) from error
        validation = phases['validation']
        score = validation.get('score')
        score_direction = validation.get('score_direction')
        if score and score_direction not in {'min', 'max'}:
            raise BackendError(
                'explore.validation.score_direction must be min|max when '
                'explore.validation.score is set')
        identifier = campaign_id(profile_name or objective)
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
                optimization = phases['optimization']
                fix = phases['fix']
                controller = phases['controller']
                merge = phases['merge']
                parallel = int(optimization.get('parallel', 4))
                max_fixes = int(fix.get('max_fixes', 3))
                max_accepted_attempts = int(
                    merge.get('max_accepted_attempts', 6))
                max_time = _duration(controller.get('max_wall_time', 28800))
                max_files = int(optimization.get('max_files', 5))
                max_lines = int(optimization.get('max_lines', 300))
                min_improvement = float(
                    validation.get('min_improvement', 0))
                validation_gpus = int(validation.get('gpus', 0))
                controller_interval = float(controller.get('interval', 5))
                controller_timeout = float(controller.get('heartbeat_timeout', 30))
                timeouts = {
                    name: float(phases[name].get('timeout', 1800))
                    for name in (
                        'planning', 'optimization', 'fix', 'validation',
                        'merge')
                }
            except (TypeError, ValueError) as error:
                raise BackendError(
                    'invalid exploration setting: {}'.format(error)) from error
            if min([parallel, controller_interval, controller_timeout] +
                   list(timeouts.values())) <= 0:
                raise BackendError(
                    'parallelism and controller limits must be positive')
            if min(max_fixes, max_accepted_attempts, max_time,
                   max_files, max_lines) < 0:
                raise BackendError('exploration maximums cannot be negative')
            if validation_gpus < 0:
                raise BackendError('validation GPUs cannot be negative')
            if min_improvement < 0:
                raise BackendError('minimum improvement cannot be negative')
            budgets = {
                'parallel': parallel,
                'max_fixes': max_fixes,
                'max_accepted_attempts': max_accepted_attempts,
                'max_wall_time': max_time,
                'deadline': time.time() + max_time if max_time else None,
            }
            protected = list(optimization.get('protected', []))
            if '.tq/**' not in protected:
                protected.append('.tq/**')
            checks = list(validation.get('checks', []))
            for command_text in checks + ([score] if score else []):
                for token in shlex.split(command_text):
                    path = (Path(root) / token).resolve()
                    try:
                        relative = path.relative_to(Path(root))
                    except ValueError:
                        continue
                    if path.is_file():
                        protected.append(str(relative))
            asset_source = (
                Path(root) / '.tq' / 'explore' / profile_name / 'assets'
                if profile_name else None)
            asset_manifest = inventory(asset_source) if asset_source else []
            asset_snapshot = work_root / 'assets-snapshot'
            if asset_manifest:
                copy_assets(asset_source, asset_snapshot, asset_manifest)
            campaign_config = {
                'repo_root': root,
                'work_root': str(work_root),
                'mainline_branch': mainline_branch,
                'mainline_worktree': str(mainline_worktree),
                'control_cwd': str(control_cwd),
                'heartbeat_file': str(heartbeat_file),
                'backend_config': _plain(self.backend.config),
                'profile_name': profile_name,
                'env': campaign_environment,
                'asset_snapshot': str(asset_snapshot),
                'asset_manifest': asset_manifest,
                'phases': {
                    'planning': {
                        'command': commands['planning'],
                        'prompt': _required_text(
                            phases['planning'], 'prompt', 'planning'),
                        'response_repair_prompt': _required_text(
                            phases['planning'], 'response_repair_prompt',
                            'planning'),
                        'timeout': timeouts['planning'],
                    },
                    'optimization': {
                        'command': commands['optimization'],
                        'prompt': _required_text(
                            optimization, 'prompt', 'optimization'),
                        'max_files': max_files,
                        'max_lines': max_lines,
                        'protected_paths': list(dict.fromkeys(protected)),
                        'timeout': timeouts['optimization'],
                    },
                    'fix': {
                        'command': commands['fix'],
                        'prompt': _required_text(
                            fix, 'prompt', 'fix'),
                        'response_repair_prompt': _required_text(
                            fix, 'response_repair_prompt', 'fix'),
                        'timeout': timeouts['fix'],
                    },
                    'validation': {
                        'gpus': validation_gpus,
                        'checks': checks,
                        'score': score,
                        'score_direction': score_direction,
                        'min_improvement': min_improvement,
                        'timeout': timeouts['validation'],
                    },
                    'merge': {
                        'command': commands['merge'],
                        'prompt': _required_text(merge, 'prompt', 'merge'),
                        'timeout': timeouts['merge'],
                    },
                    'controller': {
                        'interval': controller_interval,
                        'heartbeat_timeout': controller_timeout,
                        'max_wall_time': max_time,
                    },
                },
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
                campaign_config['phases']['controller']['heartbeat_timeout'])
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

    def profile_campaigns(self, root, profile_name):
        path = self._state_path(root)
        if not path.exists():
            return []
        with ExploreState(path) as state:
            return [
                campaign for campaign in state.list_campaigns()
                if campaign['config'].get('profile_name') == profile_name
            ]

    def remove_finished_profile_campaigns(self, root, profile_name):
        campaigns = self.profile_campaigns(root, profile_name)
        active = [
            campaign['id'] for campaign in campaigns
            if campaign['status'] not in {'completed', 'failed'}
        ]
        if active:
            raise BackendError(
                'profile has active campaigns: {}'.format(', '.join(active)))
        for campaign in campaigns:
            self.backend.unregister_controller(campaign['id'])
            config = campaign['config']
            work_root = config.get('work_root')
            if work_root:
                git_ref_utils.remove_nested_worktrees(work_root)
                shutil.rmtree(work_root, ignore_errors=True)
        path = self._state_path(root)
        if campaigns and path.exists():
            with ExploreState(path) as state:
                for campaign in campaigns:
                    for job in state.list_jobs(campaign_id=campaign['id']):
                        try:
                            self.backend.remove({'id': int(job['backend_job_id'])})
                        except (BackendError, FileNotFoundError, OSError):
                            pass
                state.delete_campaigns(
                    [campaign['id'] for campaign in campaigns])
        return campaigns

    def status(self, campaign=None):
        root = repository(Path.cwd())[0]
        with self._open(root) as state:
            value = self._latest(state, campaign)
            return {
                'campaign': _public_campaign(value),
                'manual_landing': value['config'].get('manual_landing'),
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
                    artifact_jobs = [
                        job for job in state.list_jobs(attempt_id=item['id'])
                        if job['role'] in {'optimizer', 'fix'}
                    ]
                    for job in reversed(artifact_jobs):
                        patch = job['metadata'].get(
                            'artifacts', {}).get('diff', '')
                        if patch:
                            break
                result.append(dict(item, diff=patch))
            return {'campaign': _public_campaign(value), 'attempts': result}

    def set_status(self, campaign, status):
        root = repository(Path.cwd())[0]
        with self._open(root) as state:
            value = self._latest(state, campaign)
            current = value['status']
            if current in {'completed', 'failed'}:
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
            config['heartbeat_file'],
            config['phases']['controller']['heartbeat_timeout'])

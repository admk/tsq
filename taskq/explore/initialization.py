"""Agent-assisted initialization of new exploration profiles."""

import argparse
import json
import os
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from string import Template

import tomlkit

from ..backends import git_ref as git_ref_utils
from ..backends.base import BackendError
from .agent import parse_command_template, render_command
from .assets import copy_assets, inventory
from .environment import (
    ENVIRONMENT_NAME,
    EnvironmentConfigError,
    resolve_environment,
)
from .git import git
from .profiles import ExploreProfile, ExploreProfileStore
from .wizard import import_generated_values, validate_generated_document


class InitializationInterrupted(Exception):
    pass


class InitializationStale(Exception):
    pass


class ProfileInitializer:
    """Run the agent-assisted setup lifecycle inside a tq worker job."""

    def __init__(
        self, store, profile, config, objective_prompt=None, run_token=None,
        stream=None,
    ):
        self.store, self.profile = store, profile
        self.config = dict(config or {})
        self.objective_prompt = objective_prompt
        self.run_token = run_token
        self.stream = stream

    def _print(self, value):
        print(value, file=self.stream)

    def run(self):
        command = self.config.get('command')
        if not command:
            self._fallback('missing initialization command', 0)
            return False
        try:
            command = parse_command_template(command)
            timeout = float(self.config.get('timeout', 600))
            if timeout <= 0:
                raise ValueError('initialization timeout must be positive')
        except (TypeError, ValueError) as error:
            self._fallback(str(error), 0)
            return False
        root = self.store.root
        try:
            head = git(root, 'rev-parse', 'HEAD')
        except BackendError as head_error:
            # A healthy worktree without a resolvable HEAD is an unborn
            # branch, even if another ref in the repository has commits.
            try:
                git(root, 'rev-parse', '--verify', 'HEAD')
            except BackendError:
                try:
                    git(root, 'status', '--porcelain', '--untracked-files=no')
                except BackendError:
                    raise head_error
            else:
                raise head_error
            self._fallback(
                'current branch has no commit; continuing with manual profile setup',
                0)
            return False
        dirty = bool(git(root, 'status', '--porcelain', '--untracked-files=normal'))
        if dirty:
            self._print('Local changes ignored; generating from committed HEAD {}.'.format(
                head[:12]))
        state = self._save_generation(
            self.profile,
            status=('running' if os.environ.get('TASKQ_JOB_ID') else 'pending'),
            source_commit=head,
            attempts=0, errors=[], asset_hashes=[])
        log_path = self.profile.path / 'generation.log'
        initialization_env = {
            str(key): str(value)
            for key, value in self.store.resolved_config.get('env', {}).items()
            if (value and ENVIRONMENT_NAME.fullmatch(str(key)) and
                '\0' not in str(value))
        }
        initialization_env.update(os.environ)
        initialization_env.update({
            'TASKQ_REPO_ROOT': str(root),
        })
        campaign_base_environment = dict(initialization_env)
        try:
            initialization_env.update(resolve_environment(
                self.profile.document.get('explore', {}).get('env', {}),
                root, initialization_env))
        except EnvironmentConfigError as error:
            self._fallback(str(error), 0)
            return False
        worktree = Path(tempfile.mkdtemp(prefix='tq-explore-init-'))
        initialization_env['TASKQ_INIT_WORKTREE'] = str(worktree)
        environment_context = {
            key: initialization_env[key]
            for key in (
                'TASKQ_REPO_ROOT', 'VIRTUAL_ENV', 'CONDA_PREFIX',
                'UV_PROJECT_ENVIRONMENT', 'PYTHONPATH', 'PATH')
            if initialization_env.get(key)
        }
        registered = False
        try:
            shutil.rmtree(worktree)
            git_ref_utils.create_worktree(root, head, worktree)
            registered = True
            relative = Path('.tq') / 'explore' / self.profile.name
            generated_path = worktree / relative
            generated_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(self.profile.path, generated_path)
            protected_files = self._protected_profile_files(generated_path)
            error_text = ''
            for attempt in (1, 2):
                template_key = 'prompt' if attempt == 1 else 'repair_prompt'
                template = self.config.get(template_key)
                if not isinstance(template, str) or not template.strip():
                    error_text = 'missing initialization {}'.format(template_key)
                    break
                if attempt == 2:
                    template = '{}\n\n{}'.format(
                        self.config.get('prompt', ''), template)
                state['attempts'] = attempt
                objective_prompt = (
                    self.profile.objective
                    if self.objective_prompt is None
                    else str(self.objective_prompt).strip()
                )
                prompt = Template(template).safe_substitute(
                    objective_prompt=objective_prompt,
                    # Keep custom pre-objective.md templates working. The
                    # generated campaign objective still comes only from
                    # objective.md and is imported separately below.
                    objective=objective_prompt,
                    profile_path=relative.as_posix(),
                    repository=str(worktree), source_root=str(root),
                    host_environment=json.dumps(
                        environment_context, indent=2, sort_keys=True),
                    error=error_text)
                self._print(
                    'Generating profile settings (attempt {}, elapsed 0s)…'.format(
                        attempt))
                started = time.monotonic()
                process = None
                try:
                    process = subprocess.Popen(
                        render_command(command, prompt), cwd=worktree,
                        env=initialization_env)
                    process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    error_text = 'initialization agent timed out after {} seconds'.format(
                        timeout)
                    self._append_log(log_path, attempt, error_text)
                    break
                except KeyboardInterrupt as error:
                    if process is not None:
                        process.terminate()
                        try:
                            process.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                    self._append_log(log_path, attempt, 'interrupted')
                    self._save_generation(
                        self.profile, status='interrupted', attempts=attempt,
                        errors=['interrupted'])
                    raise InitializationInterrupted() from error
                self._append_log(
                    log_path, attempt,
                    'Agent output is available from tq job {}.'.format(
                        os.environ.get('TASKQ_JOB_ID', 'output')))
                self._print('Agent finished in {:.1f}s.'.format(
                    time.monotonic() - started))
                state['attempts'] = attempt
                if process.returncode:
                    error_text = 'initialization agent exited with status {}'.format(
                        process.returncode)
                    break
                try:
                    self._guard_changes(
                        worktree, generated_path, head, protected_files)
                    generated = self._load_generated(generated_path)
                    validate_generated_document(self.profile.document, generated.document)
                    resolve_environment(
                        generated.document.get('explore', {}).get('env', {}),
                        root, campaign_base_environment)
                    assets = inventory(generated_path / 'assets')
                    self._complete_generation(
                        generated, generated_path / 'assets', assets,
                        attempt, head)
                    self._summary(assets)
                    return True
                except (BackendError, OSError, ValueError, TypeError) as error:
                    error_text = str(error)
                    state.setdefault('errors', []).append(error_text)
            self._fallback(
                error_text or 'profile generation failed', state['attempts'],
                state.get('errors'))
            return False
        finally:
            if registered:
                git_ref_utils.remove_worktree({
                    'git_root': str(root), 'git_worktree': str(worktree)}, force=True)
            else:
                shutil.rmtree(worktree, ignore_errors=True)

    def _load_generated(self, path):
        config_path = path / 'config.toml'
        try:
            with open(config_path, 'r', encoding='utf-8') as stream:
                document = tomlkit.load(stream)
        except (OSError, tomlkit.exceptions.ParseError) as error:
            raise ValueError('invalid generated config: {}'.format(error)) from error
        return ExploreProfile(self.profile.name, path, document)

    def _guard_changes(
        self, worktree, generated_path, source_head, protected_files,
    ):
        if git(worktree, 'rev-parse', 'HEAD') != source_head:
            raise ValueError('initialization agent changed repository HEAD')
        status = git(
            worktree, 'status', '--porcelain', '--untracked-files=all')
        if status:
            raise ValueError(
                'initialization agent changed files outside the profile: {}'.format(
                    status.splitlines()[0][3:]))
        expected = protected_files
        actual = self._protected_profile_files(generated_path)
        if actual != expected:
            changed = sorted(set(actual) ^ set(expected))
            if not changed:
                changed = sorted(
                    key for key in actual if actual[key] != expected[key])
            raise ValueError(
                'initialization agent changed protected profile file: {}'.format(
                    changed[0] if changed else 'unknown'))

    @staticmethod
    def _protected_profile_files(root):
        root = Path(root)
        result = {}
        for path in sorted(root.rglob('*')):
            relative = path.relative_to(root)
            if relative in {Path('config.toml'), Path('objective.md')} or (
                relative.parts[:1] == ('assets',)
            ):
                continue
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                continue
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                raise ValueError(
                    'protected profile entries must be regular files: {}'.format(
                        relative))
            result[relative.as_posix()] = (
                info.st_mode & 0o777, path.read_bytes())
        return result

    def _import(self, generated, asset_root, assets):
        generated_objective = generated.objective
        import_generated_values(self.profile.document, generated.document)
        self.profile.metadata['cursor'] = 0
        self.store.save(self.profile)
        self.store.save_objective(self.profile, generated_objective)
        copy_assets(
            asset_root, self.profile.path / 'assets', expected=assets)

    def _complete_generation(
        self, generated, asset_root, assets, attempt, source_commit,
    ):
        values = {
            'status': 'generated',
            'attempts': attempt,
            'errors': [],
            'asset_hashes': assets,
            'source_commit': source_commit,
        }
        if self.run_token is None:
            self._import(generated, asset_root, assets)
            self.store.save_generation(self.profile, **values)
            return
        with self.store.edit_generation(
            self.profile, self.run_token,
        ) as generation:
            if generation is None:
                raise InitializationStale()
            # Keep the token check and profile import in one critical section;
            # a superseded worker must never overwrite the newer request.
            self._import(generated, asset_root, assets)
            generation.update(values)

    def _save_generation(self, profile, **values):
        if self.run_token is None:
            return self.store.save_generation(profile, **values)
        state = self.store.save_generation(
            profile, expected_run_token=self.run_token, **values)
        if state is None:
            raise InitializationStale()
        return state

    @staticmethod
    def _append_log(path, attempt, output):
        with open(path, 'a', encoding='utf-8') as stream:
            stream.write('\n=== attempt {} ===\n{}'.format(attempt, output or ''))

    def _fallback(self, error, attempts, errors=None):
        log_path = self.profile.path / 'generation.log'
        values = {
            'status': 'fallback',
            'attempts': attempts,
            'errors': list(errors or []) + (
                [] if errors and errors[-1:] == [error] else [error]),
        }
        if self.run_token is None:
            if not log_path.exists():
                self.store._atomic_text(log_path, '')
            self.store.save_generation(self.profile, **values)
        else:
            with self.store.edit_generation(
                self.profile, self.run_token,
            ) as generation:
                if generation is None:
                    self._print('Profile generation request was superseded.')
                    return
                if not log_path.exists():
                    self.store._atomic_text(log_path, '')
                generation.update(values)
        self._print('Profile generation failed: {}'.format(error))
        job_id = os.environ.get('TASKQ_JOB_ID')
        if job_id:
            self._print('Generation output: tq outputs {}'.format(job_id))
        else:
            self._print('Generation log: {}'.format(
                self.profile.path / 'generation.log'))

    def _summary(self, assets):
        if assets:
            self._print('Generated {} asset(s): {}'.format(
                len(assets), ', '.join(item['path'] for item in assets)))
        else:
            self._print('Generated profile settings; no evaluator assets added.')


class ProfileInitializationJob:
    """Queue, attach to, and resume a profile-initialization tq job."""

    ACTIVE = frozenset({'queued', 'running'})
    RETRYABLE = frozenset({'failed', 'killed', 'interrupted'})
    FINAL_GENERATION = frozenset({'generated', 'fallback'})
    JOB_KIND = 'explore-profile-initialization'

    def __init__(
        self, store, profile, config, backend, objective_prompt=None,
        stream=None,
    ):
        self.store = store
        self.profile = profile
        self.config = dict(config or {})
        self.backend = backend
        self.objective_prompt = objective_prompt
        self.stream = stream

    def _print(self, value):
        print(value, file=self.stream)

    def run(self):
        if getattr(self.backend, 'name', None) != 'tmux':
            raise BackendError(
                'agent-assisted profile initialization requires the tmux backend')

        state = self.store.read_generation(self.profile)
        if state.get('status') == 'interrupted':
            self._pause(state.get('backend_job_id'))
        if state.get('status') in self.FINAL_GENERATION:
            return True

        if not state.get('run_token'):
            created = self._new_state(state)
            state = created or self.store.read_generation(self.profile)
        run_token = state.get('run_token')
        if not run_token:
            raise BackendError('profile generation request has no run token')

        job = self._resolve_job(state)
        if job is None and state.get('backend_job_id') is None:
            job = self._queue(state)
            if job is None:
                return True

        if job is not None and job.get('status') in self.ACTIVE:
            self._print(
                'Profile generation is running in tq job {}.'.format(job['id']))
            self._print(
                'Detach without stopping it, or reattach later with: tq interact {}'.format(
                    job['id']))
            try:
                self.backend.interact(job)
            except KeyboardInterrupt as error:
                raise InitializationInterrupted() from error

        state = self.store.read_generation(self.profile)
        job_id = state.get('backend_job_id') or (
            job.get('id') if job else None)
        return self._settle(run_token, self._job_by_id(job_id))

    def _new_state(self, previous):
        run_token = uuid.uuid4().hex
        return self.store.save_generation(
            self.profile,
            expected_run_token=previous.get('run_token'),
            status='queued',
            run_token=run_token,
            backend_job_id=None,
            config=_json_copy(self.config),
            objective_prompt=(
                self.profile.objective
                if self.objective_prompt is None
                else str(self.objective_prompt).strip()),
            attempts=0,
            errors=[],
            asset_hashes=[],
        )

    def _queue(self, state):
        run_token = state['run_token']
        command = shlex.join([
            sys.executable,
            '-m',
            'taskq.explore.initialization',
            '--worker',
            '--root',
            str(self.store.root),
            '--profile',
            self.profile.name,
            '--run-token',
            run_token,
        ])
        metadata = {
            'kind': self.JOB_KIND,
            'repo_root': str(self.store.root),
            'profile_name': self.profile.name,
            'run_token': run_token,
        }
        with self.store.edit_generation(
            self.profile, run_token,
        ) as current:
            if current is None:
                self._superseded()
            existing_id = current.get('backend_job_id')
            if existing_id is not None:
                return self._job_by_id(existing_id)
            try:
                backend_job_id = self.backend.add(
                    command,
                    gpus=0,
                    slots=1,
                    cwd=str(self.store.root),
                    metadata=metadata,
                )
            except Exception as error:
                message = 'could not queue profile generation job: {}'.format(error)
                current.update(status='fallback', errors=[message])
                self._print('Profile generation failed: {}'.format(message))
                return None
            current['backend_job_id'] = str(backend_job_id)
            return {
                'id': _backend_job_id(backend_job_id),
                'status': 'queued',
            }

    def _resolve_job(self, state):
        job_id = state.get('backend_job_id')
        if job_id is not None:
            job = self._job_by_id(job_id)
            if job is not None:
                return job
        token = state.get('run_token')
        if not token:
            return None
        job = self._matching_job(token)
        if job is None:
            return None
        if self.store.save_generation(
            self.profile, expected_run_token=token,
            backend_job_id=str(job['id']),
        ) is None:
            self._superseded()
        return job

    def _matching_job(self, token):
        for item in self.backend.full_info(None):
            metadata = item.get('metadata') or {}
            if (
                metadata.get('kind') == self.JOB_KIND and
                metadata.get('repo_root') == str(self.store.root) and
                metadata.get('profile_name') == self.profile.name and
                metadata.get('run_token') == token
            ):
                return item
        return None

    def _job_by_id(self, job_id):
        if job_id is None:
            return None
        jobs = self.backend.job_info([_backend_job_id(job_id)])
        return jobs[0] if jobs else None

    def reconcile_interrupted(self):
        """Make a stale active request editable after its job was aborted."""
        state = self.store.read_generation(self.profile)
        if state.get('status') not in {'queued', 'running', 'pending'}:
            return state
        if not (
            state.get('run_token') or
            state.get('backend_job_id') is not None
        ):
            return state
        job = self._resolve_job(state)
        if not job or job.get('status') not in self.RETRYABLE:
            return self.store.read_generation(self.profile)
        return self._mark_interrupted(state, job['status'])

    def _mark_interrupted(self, state, job_status):
        reason = 'profile generation job was {}'.format(job_status)
        errors = list(state.get('errors') or [])
        if errors[-1:] != [reason]:
            errors.append(reason)
        updated = self.store.save_generation(
            self.profile, expected_run_token=state.get('run_token'),
            status='interrupted', errors=errors)
        return updated or self.store.read_generation(self.profile)

    def _settle(self, run_token, job):
        state = self.store.read_generation(self.profile)
        if state.get('run_token') != run_token:
            self._superseded()
        if state.get('status') in self.FINAL_GENERATION:
            return True
        job_status = job.get('status') if job else None
        if job_status in self.ACTIVE:
            self._pause(job.get('id'))
        if job_status in self.RETRYABLE:
            state = self._mark_interrupted(state, job_status)
            if state.get('run_token') != run_token:
                self._superseded()
            self._pause(job.get('id'))
        reason = (
            'profile generation job ended with status {}'.format(job_status)
            if job_status else 'profile generation job is missing')
        if self.store.save_generation(
            self.profile, expected_run_token=run_token,
            status='fallback',
            errors=list(state.get('errors') or []) + [reason],
        ) is None:
            self._superseded()
        self._print('Profile generation failed: {}'.format(reason))
        if job is not None:
            self._print('Generation output: tq outputs {}'.format(job['id']))
        return True

    def _pause(self, job_id):
        if job_id is not None:
            self._print(
                'Profile generation paused. Reattach with: tq interact {}'.format(
                    job_id))
        raise InitializationInterrupted()

    def _superseded(self):
        self._print('Profile generation request was superseded.')
        raise InitializationInterrupted()


def _json_copy(value):
    return json.loads(json.dumps(value))


def _backend_job_id(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def run_worker(root, profile_name, run_token):
    """Run one persisted initialization request inside its tq job."""
    store = ExploreProfileStore(root, {'env': {}})
    try:
        profile = store.load(profile_name)
        state = store.read_generation(profile)
        if state.get('run_token') != run_token:
            print('Profile initialization request is stale.', file=sys.stderr)
            return 2
        config = state.get('config')
        if not isinstance(config, dict):
            raise BackendError('profile initialization request has invalid config')
        job_id = os.environ.get('TASKQ_JOB_ID')
        if job_id:
            state = store.save_generation(
                profile, expected_run_token=run_token,
                backend_job_id=str(job_id), status='running')
            if state is None:
                print('Profile initialization request is stale.', file=sys.stderr)
                return 2
        initializer = ProfileInitializer(
            store,
            profile,
            config,
            objective_prompt=state.get('objective_prompt'),
            run_token=run_token,
            stream=sys.stdout,
        )
        try:
            generated = initializer.run()
        except InitializationStale:
            print('Profile initialization request is stale.', file=sys.stderr)
            return 2
        except InitializationInterrupted:
            return 130
        except Exception as error:
            current = store.read_generation(profile)
            initializer._fallback(
                'profile generation worker crashed: {}'.format(error),
                int(current.get('attempts') or 0),
                current.get('errors'))
            return 1
        return 0 if generated else 1
    except (BackendError, OSError, TypeError, ValueError) as error:
        print('Profile generation worker failed: {}'.format(error), file=sys.stderr)
        return 1


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', action='store_true')
    parser.add_argument('--root')
    parser.add_argument('--profile')
    parser.add_argument('--run-token')
    args = parser.parse_args(argv)
    if not args.worker:
        parser.error('only --worker mode is supported')
    missing = [
        name for name in ('root', 'profile', 'run_token')
        if not getattr(args, name)
    ]
    if missing:
        parser.error('missing worker arguments: {}'.format(', '.join(missing)))
    return run_worker(args.root, args.profile, args.run_token)


if __name__ == '__main__':
    sys.exit(main())

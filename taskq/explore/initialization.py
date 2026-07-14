"""Agent-assisted initialization of new exploration profiles."""

import json
import os
import shutil
import stat
import subprocess
import tempfile
import time
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
from .profiles import ExploreProfile
from .wizard import import_generated_values, validate_generated_document


class InitializationInterrupted(Exception):
    pass


class ProfileInitializer:
    def __init__(self, store, profile, config, stream=None):
        self.store, self.profile = store, profile
        self.config = dict(config or {})
        self.stream = stream

    def _print(self, value):
        print(value, file=self.stream)

    def run(self):
        command = self.config.get('command')
        if not command:
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
        head = git(root, 'rev-parse', 'HEAD')
        dirty = bool(git(root, 'status', '--porcelain', '--untracked-files=normal'))
        if dirty:
            self._print('Local changes ignored; generating from committed HEAD {}.'.format(
                head[:12]))
        state = self.store.save_generation(
            self.profile, status='pending', source_commit=head,
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
                prompt = Template(template).safe_substitute(
                    objective=self.profile.objective,
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
                        env=initialization_env,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        text=True)
                    output, _ = process.communicate(timeout=timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    output, _ = process.communicate()
                    error_text = 'initialization agent timed out after {} seconds'.format(
                        timeout)
                    self._append_log(log_path, attempt, output)
                    break
                except KeyboardInterrupt as error:
                    output = ''
                    if process is not None:
                        process.terminate()
                        try:
                            output, _ = process.communicate(timeout=2)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            output, _ = process.communicate()
                    self._append_log(log_path, attempt, output)
                    self.store.save_generation(
                        self.profile, status='interrupted', attempts=attempt,
                        errors=['interrupted'])
                    raise InitializationInterrupted() from error
                self._append_log(log_path, attempt, output)
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
                    self._import(generated, generated_path / 'assets', assets)
                    self.store.save_generation(
                        self.profile, status='generated', attempts=attempt,
                        errors=[], asset_hashes=assets, source_commit=head)
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
            if relative == Path('config.toml') or relative.parts[:1] == ('assets',):
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
        import_generated_values(self.profile.document, generated.document)
        self.profile.metadata['cursor'] = 0
        self.store.save(self.profile)
        copy_assets(asset_root, self.profile.path / 'assets', expected=assets)

    @staticmethod
    def _append_log(path, attempt, output):
        with open(path, 'a', encoding='utf-8') as stream:
            stream.write('\n=== attempt {} ===\n{}'.format(attempt, output or ''))

    def _fallback(self, error, attempts, errors=None):
        log_path = self.profile.path / 'generation.log'
        if not log_path.exists():
            self.store._atomic_text(log_path, '')
        self.store.save_generation(
            self.profile, status='fallback', attempts=attempts,
            errors=list(errors or []) + (
                [] if errors and errors[-1:] == [error] else [error]))
        self._print('Profile generation failed: {}'.format(error))
        self._print('Generation log: {}'.format(
            self.profile.path / 'generation.log'))

    def _summary(self, assets):
        if assets:
            self._print('Generated {} asset(s): {}'.format(
                len(assets), ', '.join(item['path'] for item in assets)))
        else:
            self._print('Generated profile settings; no evaluator assets added.')

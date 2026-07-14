"""Project-local exploration profiles and prompt files."""

import copy
import fcntl
import json
import os
import re
import shutil
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import tomlkit

from ..backends.base import BackendError
from ..common import dict_merge


PROFILE_VERSION = 1
PROFILE_NAME = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]*$')
OBJECTIVE_FILENAME = 'objective.md'
PHASES = (
    'planning', 'optimization', 'inspection', 'validation',
    'merge', 'controller',
)
PROMPTS = (
    ((), 'response_repair_prompt', 'response-repair.md'),
    (('planning',), 'prompt', 'planning.md'),
    (('optimization',), 'prompt', 'optimization.md'),
    (('optimization',), 'adjust_prompt', 'adjustment.md'),
    (('inspection',), 'prompt', 'inspection.md'),
    (('merge',), 'review_prompt', 'merge-review.md'),
    (('merge',), 'rebase_prompt', 'rebase.md'),
)
ASSET_MAX_FILES = 64
ASSET_MAX_BYTES = 2 * 1024 * 1024
_ANY_RUN_TOKEN = object()


def _objective_path(profile_path):
    root = Path(profile_path)
    if not root.is_dir():
        raise BackendError('profile directory not found: {}'.format(root))
    return root / OBJECTIVE_FILENAME


def _objective_text(value):
    if not isinstance(value, str):
        raise BackendError('profile objective must be text')
    value = value.replace('\r\n', '\n').replace('\r', '\n')
    value = value.strip()
    if not value:
        raise BackendError('profile objective cannot be empty')
    return value


def read_objective(profile_path):
    """Read and validate a profile-root objective.md file."""
    path = _objective_path(profile_path)
    if not path.exists():
        raise BackendError('profile objective file not found: {}'.format(path))
    if not path.is_file():
        raise BackendError('profile objective must be a regular file: {}'.format(path))
    try:
        value = path.read_text(encoding='utf-8')
    except UnicodeDecodeError as error:
        raise BackendError(
            'profile objective must be valid UTF-8: {}'.format(path)) from error
    except OSError as error:
        raise BackendError(
            'profile objective file is not readable: {}'.format(path)) from error
    return _objective_text(value)


def write_objective(profile_path, value):
    """Validate and atomically write a profile-root objective.md file."""
    path = _objective_path(profile_path)
    value = _objective_text(value)
    if path.exists() and not path.is_file():
        raise BackendError('profile objective must be a regular file: {}'.format(path))
    ExploreProfileStore._atomic_text(path, value + '\n')
    return value


def _plain(value):
    return json.loads(json.dumps(value))


def _nested(mapping, path, create=False):
    for key in path:
        if create:
            mapping = mapping.setdefault(key, {})
        else:
            mapping = mapping[key]
    return mapping


def get_value(mapping, path, default=None):
    try:
        parent = _nested(mapping, path[:-1])
        return parent.get(path[-1], default)
    except (KeyError, TypeError):
        return default


def set_value(mapping, path, value):
    _nested(mapping, path[:-1], create=True)[path[-1]] = value


def delete_value(mapping, path):
    try:
        del _nested(mapping, path[:-1])[path[-1]]
    except (KeyError, TypeError):
        pass


def resolve_phases(explore):
    explore = _plain(explore)
    common = {key: value for key, value in explore.items() if key not in PHASES}
    return {
        name: dict(common, **dict(explore.get(name) or {}))
        for name in PHASES
    }


@dataclass
class ExploreProfile:
    name: str
    path: Path
    document: object

    @property
    def metadata(self):
        return self.document['profile']

    @property
    def objective(self):
        return read_objective(self.path)

    @property
    def complete(self):
        return bool(self.metadata.get('complete', False))

    @property
    def cursor(self):
        return int(self.metadata.get('cursor', 0))


class ExploreProfileStore:
    """Create, update, load, and remove named project profiles."""

    _generation_thread_locks = {}
    _generation_thread_locks_guard = threading.Lock()

    def __init__(self, root, resolved_config):
        self.root = Path(root).resolve()
        self.resolved_config = resolved_config
        self.base = self.root / '.tq' / 'explore'

    @staticmethod
    def validate_name(name):
        if not isinstance(name, str) or not PROFILE_NAME.fullmatch(name):
            raise BackendError(
                'profile name must start with a letter or digit and contain only '
                'letters, digits, dot, underscore, or hyphen')
        return name

    def profile_dir(self, name):
        name = self.validate_name(name)
        path = self.base / name
        if path.is_symlink():
            raise BackendError('profile directory cannot be a symbolic link: {}'.format(path))
        return path

    def list(self):
        if not self.base.is_dir():
            return []
        result = []
        for path in sorted(self.base.iterdir(), key=lambda item: item.name.lower()):
            if path.is_dir() and not path.is_symlink() and (path / 'config.toml').is_file():
                result.append(path.name)
        return result

    def create(self, name):
        path = self.profile_dir(name)
        config_path = path / 'config.toml'
        if config_path.exists():
            profile = self.load(name)
            self._ensure_prompt_files(profile)
            return profile
        path.mkdir(parents=True, exist_ok=True)
        explore = _plain(self.resolved_config.get('explore', {}))
        # Initialization is a local bootstrap facility, not campaign config.
        explore.pop('initialization', None)
        phases = resolve_phases(explore)
        prompt_dir = path / 'prompts'
        prompt_dir.mkdir(exist_ok=True)
        for phase_path, key, filename in PROMPTS:
            source = get_value(
                phases[phase_path[0]] if phase_path else explore,
                (key,),
            )
            if not isinstance(source, str) or not source.strip():
                raise BackendError('missing default exploration prompt: {}'.format(key))
            prompt_path = prompt_dir / filename
            if not prompt_path.exists():
                self._atomic_text(prompt_path, source.rstrip() + '\n')
            target = _nested(explore, phase_path, create=True)
            target.pop(key, None)
            explore.pop(key, None)
            target[key + '_file'] = 'prompts/' + filename
        default_objective = re.sub(r'[-_]+', ' ', name).strip()
        document = tomlkit.document()
        document['profile'] = {
            'version': PROFILE_VERSION,
            'name': name,
            'complete': False,
            'cursor': 0,
        }
        document['explore'] = explore
        write_objective(path, default_objective)
        self.save_document(path, document)
        return ExploreProfile(name, path, document)

    def _ensure_prompt_files(self, profile):
        explore = profile.document['explore']
        defaults = _plain(self.resolved_config.get('explore', {}))
        phases = resolve_phases(defaults)
        changed = False
        for phase_path, key, filename in PROMPTS:
            target = _nested(explore, phase_path, create=True)
            file_key = key + '_file'
            relative = target.get(file_key)
            if not relative:
                relative = 'prompts/' + filename
                target[file_key] = relative
                changed = True
            prompt_path = self._safe_prompt_path(profile.path, relative)
            if prompt_path.exists():
                continue
            source = get_value(
                phases[phase_path[0]] if phase_path else defaults, (key,))
            if not isinstance(source, str) or not source.strip():
                raise BackendError('missing default exploration prompt: {}'.format(key))
            self._atomic_text(prompt_path, source.rstrip() + '\n')
        if changed:
            self.save(profile)

    def load(self, name):
        path = self.profile_dir(name)
        config_path = path / 'config.toml'
        try:
            with open(config_path, 'r', encoding='utf-8') as stream:
                document = tomlkit.load(stream)
        except FileNotFoundError as error:
            raise BackendError('exploration profile not found: {}'.format(name)) from error
        except tomlkit.exceptions.ParseError as error:
            raise BackendError('invalid profile {}: {}'.format(config_path, error)) from error
        metadata = document.get('profile') or {}
        if metadata.get('name') != name or metadata.get('version') != PROFILE_VERSION:
            raise BackendError('invalid profile metadata: {}'.format(config_path))
        if not isinstance(document.get('explore'), dict):
            raise BackendError('profile is missing [explore]: {}'.format(config_path))
        profile = ExploreProfile(name, path, document)
        self._migrate_objective(profile)
        return profile

    def _migrate_objective(self, profile):
        marker = object()
        legacy = profile.metadata.get('objective', marker)
        path = profile.path / OBJECTIVE_FILENAME
        if not path.exists():
            if legacy is marker:
                raise BackendError(
                    'profile objective file not found: {}'.format(path))
            self.save_objective(profile, legacy)
        self.read_objective(profile)
        if legacy is not marker:
            del profile.metadata['objective']
            self.save(profile)

    def save(self, profile):
        self.save_document(profile.path, profile.document)

    def save_document(self, path, document):
        self._atomic_text(Path(path) / 'config.toml', tomlkit.dumps(document))

    @staticmethod
    def read_objective(profile):
        return read_objective(profile.path)

    @staticmethod
    def save_objective(profile, value):
        return write_objective(profile.path, value)

    def effective_config(self, profile):
        config = copy.deepcopy(self.resolved_config)
        profile_explore = _plain(profile.document['explore'])
        for phase_path, key, _filename in PROMPTS:
            target = _nested(profile_explore, phase_path, create=True)
            file_key = key + '_file'
            relative = target.pop(file_key, None)
            if not relative:
                raise BackendError(
                    'profile is missing explore.{}{}'.format(
                        '.'.join(phase_path) + '.' if phase_path else '', file_key))
            target[key] = self._read_prompt(profile.path, relative)
        dict_merge(config, {'explore': profile_explore})
        return config

    def generation_path(self, profile):
        return profile.path / 'generation.json'

    def read_generation(self, profile):
        with self.generation_lock(profile):
            return self._read_generation_unlocked(profile)

    def save_generation(
        self, profile, expected_run_token=_ANY_RUN_TOKEN, **values,
    ):
        with self.edit_generation(profile, expected_run_token) as current:
            if current is None:
                return None
            current.update(values)
            return current

    @contextmanager
    def edit_generation(self, profile, expected_run_token=_ANY_RUN_TOKEN):
        """Lock, conditionally edit, and atomically persist generation state."""
        with self.generation_lock(profile):
            current = self._read_generation_unlocked(profile)
            if (
                expected_run_token is not _ANY_RUN_TOKEN and
                current.get('run_token') != expected_run_token
            ):
                yield None
                return
            yield current
            self._write_generation_unlocked(profile, current)

    @contextmanager
    def generation_lock(self, profile):
        """Serialize generation-state handoffs across threads and processes."""
        # Keep the lock outside the removable profile directory. Otherwise a
        # removal can unlink the locked inode and let an old worker acquire a
        # different lock while recreating the same profile path.
        path = self.base / '.locks' / '{}.generation.lock'.format(profile.name)
        key = str(path.resolve())
        with self._generation_thread_locks_guard:
            thread_lock = self._generation_thread_locks.setdefault(
                key, threading.RLock())
        with thread_lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'a+', encoding='utf-8') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock, fcntl.LOCK_UN)

    def _read_generation_unlocked(self, profile):
        path = self.generation_path(profile)
        if path.is_file():
            try:
                current = json.loads(path.read_text(encoding='utf-8'))
            except (OSError, ValueError):
                return {}
            return current if isinstance(current, dict) else {}
        return {}

    def _write_generation_unlocked(self, profile, current):
        path = self.generation_path(profile)
        self._atomic_text(path, json.dumps(
            current, indent=2, sort_keys=True) + '\n')

    def remove(self, name):
        path = self.profile_dir(name)
        if not path.exists():
            raise BackendError('exploration profile not found: {}'.format(name))
        profile = self.load(name)
        with self.generation_lock(profile):
            shutil.rmtree(path)

    @staticmethod
    def _atomic_text(path, value):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(
            '.{}.{}.{}.tmp'.format(path.name, os.getpid(), uuid.uuid4().hex))
        descriptor = os.open(
            str(temporary), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        try:
            stream = os.fdopen(descriptor, 'w', encoding='utf-8')
            descriptor = None
            with stream:
                stream.write(value)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _read_prompt(profile_path, relative):
        resolved = ExploreProfileStore._safe_prompt_path(profile_path, relative)
        try:
            return resolved.read_text(encoding='utf-8')
        except FileNotFoundError as error:
            raise BackendError('profile prompt file not found: {}'.format(resolved)) from error

    @staticmethod
    def _safe_prompt_path(profile_path, relative):
        relative = Path(str(relative))
        if relative.is_absolute() or '..' in relative.parts:
            raise BackendError('prompt path must stay inside the profile: {}'.format(relative))
        profile_path = Path(profile_path).resolve()
        prompt_path = profile_path / relative
        if prompt_path.is_symlink():
            raise BackendError('prompt file cannot be a symbolic link: {}'.format(prompt_path))
        resolved = prompt_path.resolve()
        try:
            resolved.relative_to(profile_path)
        except ValueError as error:
            raise BackendError('prompt path escapes the profile: {}'.format(relative)) from error
        return resolved

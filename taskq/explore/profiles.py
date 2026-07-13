"""Project-local exploration profiles and prompt files."""

import copy
import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import tomlkit

from ..backends.base import BackendError
from ..common import dict_merge


PROFILE_VERSION = 1
PROFILE_NAME = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]*$')
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
        return str(self.metadata.get('objective') or '').strip()

    @property
    def complete(self):
        return bool(self.metadata.get('complete', False))

    @property
    def cursor(self):
        return int(self.metadata.get('cursor', 0))


class ExploreProfileStore:
    """Create, update, load, and remove named project profiles."""

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
            'objective': default_objective,
            'complete': False,
            'cursor': 0,
        }
        document['explore'] = explore
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
        return ExploreProfile(name, path, document)

    def save(self, profile):
        self.save_document(profile.path, profile.document)

    def save_document(self, path, document):
        self._atomic_text(Path(path) / 'config.toml', tomlkit.dumps(document))

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

    def remove(self, name):
        path = self.profile_dir(name)
        if not path.exists():
            raise BackendError('exploration profile not found: {}'.format(name))
        shutil.rmtree(path)

    @staticmethod
    def _atomic_text(path, value):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + '.tmp')
        with open(temporary, 'w', encoding='utf-8') as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)

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

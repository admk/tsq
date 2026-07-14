"""Validation and campaign-start expansion for explore environments."""

import os
import re
from collections.abc import Mapping
from pathlib import Path
from string import Template


ENVIRONMENT_NAME = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


class EnvironmentConfigError(ValueError):
    pass


def validate_environment(values):
    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise EnvironmentConfigError('explore.env must be a mapping')
    result = {}

    class PlaceholderContext(dict):
        def __missing__(self, key):
            return ''

    for name, value in values.items():
        if not isinstance(name, str) or not ENVIRONMENT_NAME.fullmatch(name):
            raise EnvironmentConfigError(
                'invalid explore.env variable name: {!r}'.format(name))
        if name.startswith('TASKQ_'):
            raise EnvironmentConfigError(
                'explore.env cannot define reserved TASKQ_* variables')
        if not isinstance(value, str):
            raise EnvironmentConfigError('explore.env values must be strings')
        if '\0' in value:
            raise EnvironmentConfigError(
                'explore.env values cannot contain NUL bytes')
        try:
            Template(value).substitute(PlaceholderContext())
        except ValueError as error:
            raise EnvironmentConfigError(
                'invalid explore.env value for {}: {}'.format(
                    name, error)) from error
        for match in Template.pattern.finditer(value):
            reference = match.group('named') or match.group('braced')
            if reference == 'TASKQ_INIT_WORKTREE':
                raise EnvironmentConfigError(
                    'explore.env cannot reference TASKQ_INIT_WORKTREE')
        result[name] = value
    return result


def resolve_environment(values, root, environ=None):
    configured = validate_environment(values)
    inherited = dict(os.environ if environ is None else environ)
    inherited['TASKQ_REPO_ROOT'] = str(Path(root).resolve())
    resolved = {}
    resolving = []

    def resolve(name):
        if name in resolved:
            return resolved[name]
        if name not in configured:
            try:
                return inherited[name]
            except KeyError as error:
                raise EnvironmentConfigError(
                    'explore.env references undefined variable {}'.format(
                        name)) from error
        if name in resolving:
            if resolving[-1] == name and name in inherited:
                return inherited[name]
            chain = resolving[resolving.index(name):] + [name]
            raise EnvironmentConfigError(
                'cyclic explore.env reference: {}'.format(' -> '.join(chain)))

        class Context(dict):
            def __missing__(self, key):
                return resolve(key)

        resolving.append(name)
        context = Context({
            key: value for key, value in inherited.items()
            if key not in configured
        })
        context.update(resolved)
        # A self-reference such as PATH="...:${PATH}" means the inherited
        # value, while references to other configured keys use their override.
        if name in inherited:
            context[name] = inherited[name]
        try:
            value = Template(configured[name]).substitute(context)
        except ValueError as error:
            raise EnvironmentConfigError(
                'invalid explore.env value for {}: {}'.format(name, error)) from error
        finally:
            resolving.pop()
        resolved[name] = value
        return value

    return {name: resolve(name) for name in configured}

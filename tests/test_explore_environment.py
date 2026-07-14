import pytest

from taskq.explore.environment import (
    EnvironmentConfigError,
    resolve_environment,
    validate_environment,
)
from taskq.explore.workflow import _public_campaign


def test_campaign_environment_resolves_repo_root_inherited_and_configured_values(
    tmp_path,
):
    result = resolve_environment({
        'PATH': '${VIRTUAL_ENV}/bin:${PATH}',
        'VIRTUAL_ENV': '${TASKQ_REPO_ROOT}/.venv',
        'LITERAL': '$$HOME',
    }, tmp_path, {'PATH': '/usr/bin', 'HOME': '/home/test'})

    assert result == {
        'PATH': '{}/.venv/bin:/usr/bin'.format(tmp_path.resolve()),
        'VIRTUAL_ENV': '{}/.venv'.format(tmp_path.resolve()),
        'LITERAL': '$HOME',
    }


@pytest.mark.parametrize('values, message', [
    ({'1BAD': 'value'}, 'variable name'),
    ({'TASKQ_REPO_ROOT': '/other'}, 'reserved TASKQ'),
    ({'CACHE': '${TASKQ_INIT_WORKTREE}/cache'}, 'cannot reference'),
    ({'VALUE': 1}, 'must be strings'),
    ({'VALUE': '$'}, 'invalid explore.env value'),
])
def test_campaign_environment_rejects_invalid_mappings(values, message):
    with pytest.raises(EnvironmentConfigError, match=message):
        validate_environment(values)


def test_campaign_environment_rejects_undefined_and_cyclic_references(tmp_path):
    with pytest.raises(EnvironmentConfigError, match='undefined variable MISSING'):
        resolve_environment({'VALUE': '${MISSING}'}, tmp_path, {})

    with pytest.raises(EnvironmentConfigError, match='cyclic'):
        resolve_environment({
            'FIRST': '${SECOND}', 'SECOND': '${FIRST}',
        }, tmp_path, {})


def test_public_campaign_redacts_resolved_environment_values():
    campaign = _public_campaign({
        'id': 'campaign',
        'config': {
            'env': {'TOKEN': 'secret', 'VIRTUAL_ENV': '/main/.venv'},
            'backend_config': {'private': True},
        },
    })

    assert campaign['config']['env'] == {
        'TOKEN': '<redacted>', 'VIRTUAL_ENV': '<redacted>'}
    assert 'backend_config' not in campaign['config']

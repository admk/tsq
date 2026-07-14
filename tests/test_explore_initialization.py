import io
import subprocess
from pathlib import Path

import tomlkit

from taskq.cli import CLI
from taskq.explore.initialization import ProfileInitializer
from taskq.explore.profiles import ExploreProfileStore


def git(root, *args):
    return subprocess.run(
        ['git', '-C', str(root), *args],
        capture_output=True, check=True, text=True,
    ).stdout.strip()


def test_initializer_uses_original_root_and_resolved_environment(
    tmp_path, monkeypatch,
):
    root = tmp_path / 'repo'
    root.mkdir()
    git(root, 'init', '-b', 'main')
    git(root, 'config', 'user.name', 'Taskq Tests')
    git(root, 'config', 'user.email', 'taskq@example.test')
    git(root, 'config', 'commit.gpgsign', 'false')
    (root / '.gitignore').write_text('.tq/\n', encoding='utf-8')
    (root / 'app.py').write_text('value = 1\n', encoding='utf-8')
    git(root, 'add', '.gitignore', 'app.py')
    git(root, 'commit', '-m', 'initial')

    config = tomlkit.loads(
        Path('taskq/default.toml').read_text(encoding='utf-8'))
    CLI()._hydrate_prompt_assets(config)
    config['env'] = {'TOP_LEVEL': 'configured'}
    store = ExploreProfileStore(root, config)
    profile = store.create('environment')
    profile.document['explore']['env'] = {
        'VIRTUAL_ENV': '${TASKQ_REPO_ROOT}/.venv',
        'PATH': '${VIRTUAL_ENV}/bin:${PATH}',
    }
    store.save(profile)

    captured = {}

    class Process:
        returncode = 0

        @staticmethod
        def communicate(timeout=None):
            captured['timeout'] = timeout
            return 'configured profile', None

    real_popen = subprocess.Popen

    def popen(argv, *args, **kwargs):
        if argv[0] != 'setup-agent':
            return real_popen(argv, *args, **kwargs)
        captured.update({
            'argv': argv,
            'cwd': Path(kwargs['cwd']),
            'env': kwargs['env'],
        })
        return Process()

    monkeypatch.setattr(
        'taskq.explore.initialization.subprocess.Popen', popen)

    initialization_config = dict(config['explore']['initialization'])
    initialization_config.update({
        'command': ['setup-agent', '{}'], 'timeout': 30})
    result = ProfileInitializer(
        store, profile, initialization_config, stream=io.StringIO()).run()

    assert result is True
    assert captured['env']['TASKQ_REPO_ROOT'] == str(root.resolve())
    assert captured['env']['TASKQ_INIT_WORKTREE'] == str(captured['cwd'])
    assert captured['env']['TOP_LEVEL'] == 'configured'
    assert captured['env']['VIRTUAL_ENV'] == str(root.resolve() / '.venv')
    assert captured['env']['PATH'].startswith(
        str(root.resolve() / '.venv' / 'bin'))
    assert str(root.resolve()) in captured['argv'][-1]
    assert '${TASKQ_REPO_ROOT}/.venv' in captured['argv'][-1]
    assert '`$$` produces a literal dollar' in captured['argv'][-1]
    assert captured['timeout'] == 30
    assert not captured['cwd'].exists()

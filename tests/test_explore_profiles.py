import io
from contextlib import nullcontext

import pytest

from taskq.backends.base import BackendError
from taskq.explore.profiles import ExploreProfileStore
from taskq.explore.wizard import FIELDS, ExploreInitWizard, confirm_remove


def resolved_config():
    return {
        'backend': 'tmux',
        'queue': 'default',
        'explore': {
            'command': ['agent', '{}'],
            'timeout': 30,
            'response_repair_prompt': 'repair $original_prompt $error',
            'planning': {'prompt': 'plan $objective $direction_count'},
            'optimization': {
                'prompt': 'optimize $objective $direction',
                'adjust_prompt': 'adjust $objective $artifacts',
                'parallel': 2, 'max_adjustments': 1,
                'max_files': 0, 'max_lines': 100,
                'protected': ['tests/**'],
            },
            'inspection': {'prompt': 'review $objective $artifacts'},
            'validation': {'gpus': 0, 'checks': [], 'min_improvement': 0},
            'merge': {
                'review_prompt': 'merge review $objective $artifacts',
                'rebase_prompt': 'rebase $objective $artifacts',
                'max_accepted_attempts': 0,
            },
            'controller': {
                'max_wall_time': 0, 'interval': 1,
                'heartbeat_timeout': 30,
            },
        },
    }


def test_profile_scaffolds_prompts_and_loads_effective_config(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())

    profile = store.create('reduce-latency')
    config = store.effective_config(profile)

    assert profile.objective == 'reduce latency'
    assert profile.complete is False
    assert store.list() == ['reduce-latency']
    assert config['backend'] == 'tmux'
    assert config['explore']['planning']['prompt'].startswith('plan ')
    assert config['explore']['merge']['rebase_prompt'].startswith('rebase ')
    assert len(list((profile.path / 'prompts').glob('*.md'))) == 7
    assert 'prompt_file' in profile.document['explore']['planning']
    assert 'prompt' not in profile.document['explore']['planning']


def test_profile_recreate_preserves_edited_prompt(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    prompt = profile.path / 'prompts' / 'planning.md'
    prompt.write_text('custom $objective', encoding='utf-8')

    recreated = store.create('profile')

    assert prompt.read_text(encoding='utf-8') == 'custom $objective'
    assert store.effective_config(recreated)['explore']['planning']['prompt'] == (
        'custom $objective')


def test_profile_recreate_restores_only_missing_prompt(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    missing = profile.path / 'prompts' / 'planning.md'
    preserved = profile.path / 'prompts' / 'inspection.md'
    missing.unlink()
    preserved.write_text('custom review', encoding='utf-8')

    store.create('profile')

    assert missing.read_text(encoding='utf-8').startswith('plan ')
    assert preserved.read_text(encoding='utf-8') == 'custom review'


@pytest.mark.parametrize('name', ['', '../escape', 'has space', '/absolute'])
def test_profile_rejects_unsafe_names(tmp_path, name):
    store = ExploreProfileStore(tmp_path, resolved_config())

    with pytest.raises(BackendError, match='profile name'):
        store.create(name)


def test_profile_rejects_prompt_traversal(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    profile.document['explore']['planning']['prompt_file'] = '../outside.md'
    store.save(profile)

    with pytest.raises(BackendError, match='inside the profile'):
        store.effective_config(store.load('profile'))


class Style(str):
    def __call__(self, value):
        return str(value)


class FakeTerminal:
    home = '<HOME>'
    clear = '<CLEAR>'
    clear_eol = ''
    normal_cursor = '<CURSOR>'
    normal = Style('<NORMAL>')
    bold = Style('<BOLD>')
    yellow = '<ORANGE>'
    cyan = '<CYAN>'
    dim = red = Style('')

    @staticmethod
    def fullscreen():
        raise AssertionError('inline prompts must not use the alternate screen')

    @staticmethod
    def cbreak():
        return nullcontext()

    @staticmethod
    def hidden_cursor():
        raise AssertionError('inline prompts must keep the cursor visible')

    @staticmethod
    def move_up(count):
        return '<UP:{}>'.format(count)

    @staticmethod
    def move_down(count):
        return '<DOWN:{}>'.format(count)

    @staticmethod
    def move_x(column):
        return '<X:{}>'.format(column)


class NoStyleTerminal(FakeTerminal):
    normal_cursor = ''
    normal = Style('')
    bold = Style('')
    yellow = ''
    cyan = ''


class Key(str):
    def __new__(cls, value, name=None):
        item = str.__new__(cls, value)
        item.name = name
        return item


def test_wizard_commits_each_enter_and_resumes_after_control_c(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    keys = iter(['\n', '\x03'])
    output = io.StringIO()
    wizard = ExploreInitWizard(
        store, profile, FakeTerminal(), lambda: next(keys), output)

    assert wizard.run() == 130
    assert '<HOME>' not in output.getvalue()
    assert '<CLEAR>' not in output.getvalue()
    assert '<CURSOR>' in output.getvalue()
    assert '<BOLD><ORANGE>' in output.getvalue()
    assert '<BOLD><CYAN>' in output.getvalue()
    assert '◉' in output.getvalue()
    assert '○' in output.getvalue()
    visible = [
        index for index, field in enumerate(FIELDS)
        if field.is_visible(profile.document)
    ]
    assert wizard._order == visible
    assert set(wizard._rows) == set(visible)
    assert all(
        row.response_line == row.prompt_line + 1
        for row in wizard._rows.values())
    resumed = store.load('profile')
    assert resumed.cursor == 1
    assert resumed.complete is False

    keys = iter(['\n'] * 40)
    output = io.StringIO()
    wizard = ExploreInitWizard(
        store, resumed, FakeTerminal(), lambda: next(keys), output)
    assert wizard.run(restart_complete=False) == 0
    assert store.load('profile').complete is True
    assert output.getvalue().endswith('\n')


def test_wizard_falls_back_to_ansi_styles_without_terminfo(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    output = io.StringIO()

    result = ExploreInitWizard(
        store, profile, NoStyleTerminal(), lambda: '\x03', output).run()

    assert result == 130
    assert '\x1b[?25h' in output.getvalue()
    assert '\x1b[1m\x1b[33m' in output.getvalue()
    assert '\x1b[1m\x1b[36m' in output.getvalue()


def test_wizard_up_moves_to_previous_item(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    profile.metadata['cursor'] = 1
    store.save(profile)
    keys = iter([Key('', 'KEY_UP'), '\x03'])
    output = io.StringIO()

    result = ExploreInitWizard(
        store, profile, FakeTerminal(), lambda: next(keys), output).run(
            restart_complete=False)

    assert result == 130
    assert store.load('profile').cursor == 0
    assert '<UP:2>' in output.getvalue()


def test_wizard_down_moves_without_committing(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    keys = iter(['x', Key('', 'KEY_DOWN'), '\x03'])

    result = ExploreInitWizard(
        store, profile, FakeTerminal(), lambda: next(keys), io.StringIO()).run()

    assert result == 130
    resumed = store.load('profile')
    assert resumed.cursor == 1
    assert resumed.objective == 'profile'


def test_wizard_does_not_onboard_controller_options():
    assert not any(field.path[:2] == ('explore', 'controller') for field in FIELDS)


def test_remove_confirmation_uses_left_right_radio():
    keys = iter([Key('', 'KEY_RIGHT'), Key('\n', 'KEY_ENTER')])

    assert confirm_remove(
        'profile', 'summary', FakeTerminal(), lambda: next(keys),
        io.StringIO()) is True

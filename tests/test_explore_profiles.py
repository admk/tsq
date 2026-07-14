import io
import copy
import json
import re
from contextlib import contextmanager, nullcontext

import pytest

from taskq.backends.base import BackendError
from taskq.explore.agent import build_planner_prompt
from taskq.explore.profiles import (
    ExploreProfileStore, read_objective, write_objective,
)
from taskq.explore.wizard import (
    FIELDS, ExploreInitWizard, _display_width, _render_text_line,
    confirm_remove, prompt_objective,
    validate_generated_document,
)


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
    assert (profile.path / 'objective.md').read_text(encoding='utf-8') == (
        'reduce latency\n')
    assert 'objective' not in profile.metadata
    assert profile.complete is False
    assert store.list() == ['reduce-latency']
    assert config['backend'] == 'tmux'
    assert config['explore']['planning']['prompt'].startswith('plan ')
    assert config['explore']['merge']['rebase_prompt'].startswith('rebase ')
    assert len(list((profile.path / 'prompts').glob('*.md'))) == 7
    assert 'prompt_file' in profile.document['explore']['planning']
    assert 'prompt' not in profile.document['explore']['planning']


def test_profile_objective_helpers_preserve_multiline_markdown(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    objective = (
        'Reduce request latency without changing the public API.\n\n'
        '- Preserve current behavior.\n'
        '- Demonstrate the improvement with the existing benchmark.'
    )

    assert write_objective(profile.path, '\n' + objective + '\n\n') == objective
    assert read_objective(profile.path) == objective
    assert store.read_objective(profile) == objective
    assert profile.objective == objective
    assert (profile.path / 'objective.md').read_text(encoding='utf-8') == (
        objective + '\n')


def test_planning_prompt_includes_complete_objective_document(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    objective = 'Reduce latency.\n\n- Preserve compatibility.\n- Use the benchmark.'
    store.save_objective(profile, objective)

    planning = store.effective_config(profile)['explore']['planning']
    prompt = build_planner_prompt(
        profile.objective, template=planning['prompt'], direction_count=2)

    assert objective in prompt


def test_profile_load_migrates_legacy_metadata_objective(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('legacy')
    (profile.path / 'objective.md').unlink()
    profile.metadata['objective'] = 'Legacy first line\n\nLegacy details.'
    store.save(profile)

    migrated = store.load('legacy')

    assert migrated.objective == 'Legacy first line\n\nLegacy details.'
    assert 'objective' not in migrated.metadata
    assert (migrated.path / 'objective.md').read_text(encoding='utf-8') == (
        'Legacy first line\n\nLegacy details.\n')


def test_profile_load_keeps_objective_file_authoritative_over_legacy_metadata(
    tmp_path,
):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    store.save_objective(profile, 'Objective from file')
    profile.metadata['objective'] = 'Stale metadata objective'
    store.save(profile)

    loaded = store.load('profile')

    assert loaded.objective == 'Objective from file'
    assert 'objective' not in loaded.metadata


@pytest.mark.parametrize('value', ['', ' \n\t', None])
def test_profile_objective_write_rejects_invalid_text(tmp_path, value):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')

    with pytest.raises(BackendError, match='objective'):
        store.save_objective(profile, value)


def test_profile_objective_normalizes_newlines_and_allows_tabs(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')

    store.save_objective(profile, 'First line\r\n\tDetail\rLast line')

    assert profile.objective == 'First line\n\tDetail\nLast line'
    assert (profile.path / 'objective.md').read_bytes() == (
        b'First line\n\tDetail\nLast line\n')


def test_profile_objective_read_rejects_invalid_utf8_and_non_regular_files(
    tmp_path,
):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    objective = profile.path / 'objective.md'
    objective.write_bytes(b'\xff')

    with pytest.raises(BackendError, match='UTF-8'):
        read_objective(profile.path)

    objective.unlink()
    objective.mkdir()
    with pytest.raises(BackendError, match='regular file'):
        read_objective(profile.path)
    with pytest.raises(BackendError, match='regular file'):
        write_objective(profile.path, 'replacement')


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


def test_generated_profile_uses_wizard_validation_and_rejects_unknown_keys(
    tmp_path,
):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    generated = copy.deepcopy(profile.document)
    generated['explore']['timeout'] = 'not-a-duration'

    with pytest.raises(ValueError):
        validate_generated_document(profile.document, generated)

    generated = copy.deepcopy(profile.document)
    generated['explore']['unknown_agent_option'] = True
    with pytest.raises(ValueError, match='unknown or non-wizard'):
        validate_generated_document(profile.document, generated)


def test_generated_profile_accepts_campaign_environment_mapping(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    generated = copy.deepcopy(profile.document)
    generated['explore']['env'] = {
        'VIRTUAL_ENV': '${TASKQ_REPO_ROOT}/.venv',
        'PATH': '${VIRTUAL_ENV}/bin:${PATH}',
    }

    validate_generated_document(profile.document, generated)

    field = next(field for field in FIELDS if field.path == ('explore', 'env'))
    assert field.display(generated).startswith('{"VIRTUAL_ENV":')


class Style(str):
    def __call__(self, value):
        return str(value)


class FakeTerminal:
    height = 12
    width = 80
    home = '<HOME>'
    clear = '<CLEAR>'
    clear_eol = ''
    normal_cursor = '<CURSOR>'
    normal = Style('<NORMAL>')
    bold = Style('<BOLD>')
    yellow = '<ORANGE>'
    cyan = '<CYAN>'
    italic = '<ITALIC>'
    dim = '<GREY>'
    red = Style('')

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
    italic = ''
    dim = ''


class NarrowTerminal(FakeTerminal):
    width = 18


class PasteModeNarrowTerminal(NarrowTerminal):
    def __init__(self):
        self.paste_context_entries = 0
        self.paste_mode_active = False

    @contextmanager
    def bracketed_paste(self):
        self.paste_context_entries += 1
        self.paste_mode_active = True
        try:
            yield
        finally:
            self.paste_mode_active = False


class Key(str):
    def __new__(cls, value, name=None):
        item = str.__new__(cls, value)
        item.name = name
        return item


def test_text_viewport_bounds_cjk_by_display_cells():
    terminal = NarrowTerminal()
    message, column = _render_text_line(
        terminal, '界' * 40, 40, placeholder='')
    shown = message.split('› ', 1)[1]

    assert _display_width('› ' + shown) <= terminal.width - 1
    assert column <= terminal.width - 1
    assert shown


def test_generation_brief_accepts_named_long_paste_without_losing_text():
    pasted = '用现有基准测试优化请求延迟' * 12
    paste_key = Key('raw bracketed-paste sequence', 'BRACKETED_PASTE')
    paste_key.text = pasted
    keys = iter([
        paste_key,
        Key('\n', 'KEY_ENTER'),
    ])
    output = io.StringIO()
    terminal = PasteModeNarrowTerminal()

    def read_key():
        assert terminal.paste_mode_active
        return next(keys)

    result = prompt_objective(
        'default', terminal, read_key, output)

    assert result == pasted
    assert terminal.paste_context_entries == 1
    assert terminal.paste_mode_active is False
    columns = [int(value) for value in re.findall(r'<X:(\d+)>', output.getvalue())]
    assert columns
    assert max(columns) <= NarrowTerminal.width - 1


def test_paste_boundary_events_are_not_inserted_into_prompt():
    keys = iter([
        Key('\x1b[200~', 'KEY_PASTE_BEGIN'),
        Key('payload', 'BRACKETED_PASTE'),
        Key('\x1b[201~', 'KEY_PASTE_END'),
        Key('\n', 'KEY_ENTER'),
    ])

    result = prompt_objective(
        'default', FakeTerminal(), lambda: next(keys), io.StringIO())

    assert result == 'payload'


def test_wizard_commits_full_long_paste_through_objective_file(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    pasted = 'Reduce latency while preserving compatibility. ' * 12
    keys = iter([
        Key(pasted, 'BRACKETED_PASTE'),
        Key('\n', 'KEY_ENTER'),
        '\x03',
    ])
    output = io.StringIO()
    terminal = PasteModeNarrowTerminal()

    def read_key():
        assert terminal.paste_mode_active
        return next(keys)

    result = ExploreInitWizard(
        store, profile, terminal, read_key, output).run()

    assert result == 130
    assert terminal.paste_context_entries == 1
    assert store.load('profile').objective == pasted.strip()
    assert (profile.path / 'objective.md').read_text(encoding='utf-8') == (
        pasted.strip() + '\n')
    columns = [int(value) for value in re.findall(r'<X:(\d+)>', output.getvalue())]
    assert columns
    assert max(columns) <= NarrowTerminal.width - 1


def test_generation_draft_abort_reprompts_before_launch(
    tmp_path, monkeypatch,
):
    import taskq.explore.initialization as initialization

    config = resolved_config()
    config['explore']['initialization'] = {'command': ['agent', '{}']}
    store = ExploreProfileStore(tmp_path, config)
    profile = store.create('profile')
    launches = []

    class StubJob:
        def __init__(
            self, job_store, job_profile, _config, backend,
            objective_prompt=None, stream=None,
        ):
            launches.append((backend, objective_prompt))
            self.store = job_store
            self.profile = job_profile

        def run(self):
            self.store.save_generation(self.profile, status='fallback')
            return True

    monkeypatch.setattr(initialization, 'ProfileInitializationJob', StubJob)
    first = ExploreInitWizard(
        store, profile, FakeTerminal(), lambda: '\x03', io.StringIO(),
        backend=object())
    first.new_profile = True

    assert first.run() == 130
    draft = json.loads(store.generation_path(profile).read_text(encoding='utf-8'))
    assert draft['status'] == 'draft'
    assert launches == []

    brief = 'Inspect benchmarks before choosing validation commands.'
    keys = iter([
        Key(brief, 'BRACKETED_PASTE'),
        Key('\n', 'KEY_ENTER'),
        '\x03',
    ])
    backend = object()
    resumed = ExploreInitWizard(
        store, store.load('profile'), FakeTerminal(), lambda: next(keys),
        io.StringIO(), backend=backend)

    assert resumed.run() == 130
    assert launches == [(backend, brief)]
    state = json.loads(store.generation_path(profile).read_text(encoding='utf-8'))
    assert state['objective_prompt'] == brief
    assert profile.objective == 'profile'


def test_killed_generation_can_revise_brief_on_first_resume(tmp_path):
    config = resolved_config()
    config['explore']['initialization'] = {'command': ['agent', '{}']}
    store = ExploreProfileStore(tmp_path, config)
    profile = store.create('profile')
    store.save_generation(
        profile, status='running', objective_prompt='old brief',
        run_token='old-token', backend_job_id='7')

    class Backend:
        name = 'tmux'

        def __init__(self):
            self.jobs = {
                7: {
                    'id': 7,
                    'status': 'killed',
                    'metadata': {
                        'kind': 'explore-profile-initialization',
                        'repo_root': str(tmp_path.resolve()),
                        'profile_name': profile.name,
                        'run_token': 'old-token',
                    },
                },
            }
            self.add_calls = []
            self.interact_calls = []

        def job_info(self, ids=None):
            return [
                {
                    'id': job['id'],
                    'status': job['status'],
                }
                for job in self.jobs.values()
                if ids is None or job['id'] in ids
            ]

        def full_info(self, ids=None):
            return [
                dict(job) for job in self.jobs.values()
                if ids is None or job['id'] in ids
            ]

        def add(self, command, **kwargs):
            job_id = 8
            self.add_calls.append((command, kwargs))
            self.jobs[job_id] = {
                'id': job_id,
                'status': 'running',
                'metadata': kwargs['metadata'],
            }
            return str(job_id)

        def interact(self, job):
            self.interact_calls.append(job['id'])
            self.jobs[job['id']]['status'] = 'success'
            store.save_generation(profile, status='fallback')

    revised = 'Use the focused benchmark and preserve public behavior.'
    keys = iter([
        Key(revised, 'BRACKETED_PASTE'),
        Key('\n', 'KEY_ENTER'),
        '\x03',
    ])
    backend = Backend()

    result = ExploreInitWizard(
        store, profile, FakeTerminal(), lambda: next(keys), io.StringIO(),
        backend=backend).run()

    assert result == 130
    assert len(backend.add_calls) == 1
    assert backend.interact_calls == [8]
    state = json.loads(store.generation_path(profile).read_text(encoding='utf-8'))
    assert state['status'] == 'fallback'
    assert state['objective_prompt'] == revised
    assert state['backend_job_id'] == '8'
    assert profile.objective == 'profile'


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
    assert '<ITALIC><GREY>' in output.getvalue()
    assert '› ' in output.getvalue()
    assert '<X:2>' in output.getvalue()
    visible = [
        index for index, field in enumerate(FIELDS)
        if field.is_visible(profile.document)
    ]
    assert wizard._order == visible
    assert set(wizard._rows) == set(visible)
    assert len(wizard._window) < len(wizard._order)
    assert all(
        wizard._rows[index].response_line ==
        wizard._rows[index].prompt_line + 1 +
        len(FIELDS[index].comment_lines)
        for index in wizard._window)
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
    assert '\x1b[3m\x1b[2m' in output.getvalue()

    output = io.StringIO()
    confirm_remove(
        'profile', 'summary', NoStyleTerminal(), lambda: '\n', output)
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
    assert '<UP:{}>'.format(
        2 + len(FIELDS[0].comment_lines)) in output.getvalue()


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


def test_wizard_tab_moves_to_next_field_without_committing(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    keys = iter(['x', '\t', '\x03'])

    result = ExploreInitWizard(
        store, profile, FakeTerminal(), lambda: next(keys), io.StringIO()).run()

    assert result == 130
    resumed = store.load('profile')
    assert resumed.cursor == 1
    assert resumed.objective == 'profile'


def test_wizard_scrolls_when_navigation_leaves_visible_window(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    keys = iter([Key('', 'KEY_DOWN')] * 5 + ['\x03'])
    wizard = ExploreInitWizard(
        store, profile, FakeTerminal(), lambda: next(keys), io.StringIO())

    assert wizard.run() == 130
    assert store.load('profile').cursor == 5
    assert 5 in wizard._window
    assert max(
        wizard._rows[index].response_line
        for index in wizard._window) < FakeTerminal.height


def test_wizard_resumes_after_committed_radio_field(tmp_path):
    store = ExploreProfileStore(tmp_path, resolved_config())
    profile = store.create('profile')
    profile.metadata['cursor'] = len(FIELDS) - 1
    store.save(profile)

    result = ExploreInitWizard(
        store, profile, FakeTerminal(), lambda: '\x03', io.StringIO()).run(
            restart_complete=False)

    assert result == 130
    assert store.load('profile').cursor == len(FIELDS) - 1


def test_wizard_does_not_onboard_controller_options():
    assert not any(field.path[:2] == ('explore', 'controller') for field in FIELDS)


def test_every_wizard_field_has_explanatory_comment():
    assert all(field.comment_lines for field in FIELDS)


def test_remove_confirmation_uses_left_right_radio():
    keys = iter([Key('', 'KEY_RIGHT'), Key('\n', 'KEY_ENTER')])
    output = io.StringIO()

    assert confirm_remove(
        'profile', 'summary', FakeTerminal(), lambda: next(keys),
        output) is True
    assert '<BOLD><CYAN>' in output.getvalue()
    assert '◉' in output.getvalue()
    assert '○' in output.getvalue()

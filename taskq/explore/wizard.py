"""Keyboard-driven onboarding for project exploration profiles."""

import json
import shlex
import sys
from contextlib import nullcontext
from dataclasses import dataclass

from blessed import Terminal
from wcwidth import wcswidth

from ..backends.base import BackendError
from .agent import parse_command_template
from .environment import validate_environment
from .profiles import delete_value, get_value, set_value


RESPONSE_PREFIX = '› '


class WizardAbort(Exception):
    pass


def _capability(term, name, *args):
    """Return a terminal capability without requiring it in test terminals."""
    capability = getattr(term, name, '')
    return capability(*args) if callable(capability) else capability


def _clear_line(term):
    return '\r' + (_capability(term, 'clear_eol') or '\x1b[K')


def _move_lines(term, amount):
    if amount == 0:
        return ''
    name = 'move_down' if amount > 0 else 'move_up'
    capability = _capability(term, name, abs(amount))
    if capability:
        return capability
    # Blessed emits an empty capability for non-TTY streams.  Keep movement
    # correct for a real terminal while allowing lightweight fake terminals.
    return '\x1b[{}{}'.format(abs(amount), 'B' if amount > 0 else 'A')


def _move_column(term, column):
    return (_capability(term, 'move_x', column) or
            '\x1b[{}G'.format(column + 1))


def _show_cursor(term):
    return _capability(term, 'normal_cursor') or '\x1b[?25h'


def _clear_eos(term):
    return _capability(term, 'clear_eos') or '\x1b[J'


def _format(term, name, fallback):
    return str(getattr(term, name, '')) or fallback


def _orange(term):
    # ANSI has no portable orange slot; terminal themes commonly use yellow
    # as their orange accent.
    return _format(term, 'yellow', '\x1b[33m')


def _prompt(term, value):
    return (_format(term, 'bold', '\x1b[1m') + _orange(term) + value +
            _format(term, 'normal', '\x1b[0m'))


def _selected(term, value):
    return (_format(term, 'bold', '\x1b[1m') +
            _format(term, 'cyan', '\x1b[36m') + value +
            _format(term, 'normal', '\x1b[0m'))


def _dim(term, value):
    return (_format(term, 'dim', '\x1b[2m') + value +
            _format(term, 'normal', '\x1b[0m'))


def _red(term, value):
    return (_format(term, 'red', '\x1b[31m') + value +
            _format(term, 'normal', '\x1b[0m'))


def _comment(term, value):
    return (_format(term, 'italic', '\x1b[3m') +
            _format(term, 'dim', '\x1b[2m') + value +
            _format(term, 'normal', '\x1b[0m'))


def _screen_text(value):
    """Render control characters without letting input change terminal rows."""
    controls = {'\n': '↵', '\r': '↵', '\t': '⇥'}
    return ''.join(
        controls.get(character, character if character.isprintable() else '�')
        for character in str(value)
    )


def _display_width(value):
    width = wcswidth(_screen_text(value))
    return max(0, width)


def _terminal_width(term):
    try:
        return max(4, int(getattr(term, 'width', 80) or 80))
    except (TypeError, ValueError):
        return 80


def _clip_text(value, width):
    """Return a display-safe prefix no wider than ``width`` cells."""
    if width <= 0:
        return ''
    value = str(value)
    end = 0
    for candidate in range(1, len(value) + 1):
        if _display_width(value[:candidate]) > width:
            break
        end = candidate
    return _screen_text(value[:end])


def _text_viewport(value, cursor, width):
    """Return a horizontal viewport and its display-cell cursor offset."""
    value = str(value)
    cursor = max(0, min(len(value), int(cursor)))
    if width <= 0:
        return '', 0

    start = cursor
    while start:
        if _display_width(value[start - 1:cursor]) > width:
            break
        start -= 1
    end = cursor
    while end < len(value):
        if _display_width(value[start:end + 1]) > width:
            break
        end += 1
    return (
        _screen_text(value[start:end]),
        _display_width(value[start:cursor]),
    )


def _render_text_line(term, value, cursor, placeholder='', error=''):
    """Render one non-wrapping response row and return its cursor column."""
    prefix_width = _display_width(RESPONSE_PREFIX)
    # Leaving one cell unused prevents terminals with eager right-margin wrap
    # from moving the form's logical one-line response onto a second row.
    available = max(1, _terminal_width(term) - prefix_width - 1)
    error_text = '  Error: ' + error if error else ''
    error_width = min(_display_width(error_text), available // 2)
    shown_error = _clip_text(error_text, error_width)
    input_width = max(1, available - _display_width(shown_error))

    if value:
        shown, cursor_offset = _text_viewport(value, cursor, input_width)
    else:
        shown = _dim(term, _clip_text(placeholder, input_width))
        cursor_offset = 0
    suffix = _red(term, shown_error) if shown_error else ''
    column = min(
        _terminal_width(term) - 1,
        prefix_width + cursor_offset,
    )
    return _clear_line(term) + RESPONSE_PREFIX + shown + suffix, column


def _input_text(key):
    """Return printable key or bracketed-paste payload text."""
    name = getattr(key, 'name', None)
    if name == 'BRACKETED_PASTE':
        value = getattr(key, 'text', None)
        if not isinstance(value, str):
            value = str(key)
        return value
    value = str(key)
    return value if not name and value.isprintable() else ''


def _paste_context(term):
    """Enable aggregate paste events when supported by the terminal object."""
    bracketed_paste = getattr(term, 'bracketed_paste', None)
    return bracketed_paste() if bracketed_paste else nullcontext()


def _key(reader):
    try:
        return reader()
    except KeyboardInterrupt as error:
        raise WizardAbort() from error


def _inline_key(reader, stream):
    try:
        key = _key(reader)
    except WizardAbort:
        print(file=stream)
        raise
    if str(key) == '\x03':
        print(file=stream)
        raise WizardAbort()
    return key


def interactive(stdin=None, stdout=None):
    return (stdin or sys.stdin).isatty() and (stdout or sys.stdout).isatty()


def _duration(value):
    value = value.strip().lower()
    scale = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    if value and value[-1:] in scale:
        return float(value[:-1]) * scale[value[-1]]
    return float(value)


def _command(value):
    return parse_command_template(value)


def _integer(value, minimum=0):
    result = int(value)
    if result < minimum:
        raise ValueError('must be at least {}'.format(minimum))
    return result


def _number(value, minimum=0):
    result = float(value)
    if result < minimum:
        raise ValueError('must be at least {}'.format(minimum))
    return result


def _positive(value):
    result = float(value)
    if result <= 0:
        raise ValueError('must be positive')
    return result


def _string_list(value):
    result = json.loads(value)
    if not isinstance(result, list) or any(not isinstance(item, str) for item in result):
        raise ValueError('must be a JSON array of strings')
    return result


def _environment(value):
    result = json.loads(value)
    return validate_environment(result)


def _text(value):
    value = value.strip()
    if not value:
        raise ValueError('cannot be empty')
    return value


@dataclass
class Field:
    label: str
    path: tuple = ()
    parser: object = _text
    kind: str = 'text'
    options: tuple = ()
    fallback: tuple = ()
    visible: object = None
    special: str = ''
    comment: object = ''

    @property
    def comment_lines(self):
        if isinstance(self.comment, str):
            return tuple(self.comment.splitlines())
        return tuple(self.comment)

    def is_visible(self, document):
        return self.visible(document) if self.visible else True

    def current(self, document):
        if self.special == 'score_enabled':
            return bool(get_value(document, ('explore', 'validation', 'score')))
        value = get_value(document, self.path)
        if value is None and self.fallback:
            value = get_value(document, self.fallback)
        if value is None and self.parser is _environment:
            value = {}
        return value

    def display(self, document):
        value = self.current(document)
        if self.kind == 'radio':
            return value
        if self.parser is _command and isinstance(value, list):
            return shlex.join(value)
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, bool):
            return 'true' if value else 'false'
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return '' if value is None else str(value)

    def commit(self, document, raw, selection=None):
        if self.special == 'score_enabled':
            if selection:
                if not get_value(document, ('explore', 'validation', 'score')):
                    set_value(
                        document, ('explore', 'validation', 'score'),
                        'python benchmark.py')
            else:
                delete_value(document, ('explore', 'validation', 'score'))
                delete_value(document, ('explore', 'validation', 'score_direction'))
            return
        value = selection if self.kind == 'radio' else self.parser(raw)
        set_value(document, self.path, value)


def _score_enabled(document):
    return bool(get_value(document, ('explore', 'validation', 'score')))


FIELDS = (
    Field(
        'Campaign objective', special='objective',
        comment='Describe the measurable improvement this campaign should pursue.'),
    Field(
        'Common agent command', ('explore', 'command'), _command,
        comment='Default agent command; the {} argument receives the prompt.'),
    Field(
        'Common timeout', ('explore', 'timeout'), _duration,
        comment='Default phase runtime limit unless that phase overrides it.'),
    Field(
        'Campaign environment (JSON object)', ('explore', 'env'), _environment,
        comment=(
            'Environment overrides sent to every campaign phase. Values may '
            'reference inherited variables and ${TASKQ_REPO_ROOT}.'),
    ),
    Field('Planning command', ('explore', 'planning', 'command'), _command,
          fallback=('explore', 'command'),
          comment='Read-only agent command that proposes optimization directions.'),
    Field('Planning timeout', ('explore', 'planning', 'timeout'), _positive,
          fallback=('explore', 'timeout'),
          comment='Maximum runtime for one planning job.'),
    Field('Optimization command', ('explore', 'optimization', 'command'), _command,
          fallback=('explore', 'command'),
          comment='Workspace-write agent command that implements each attempt.'),
    Field('Optimization timeout', ('explore', 'optimization', 'timeout'), _positive,
          fallback=('explore', 'timeout'),
          comment='Maximum runtime for one optimization or adjustment job.'),
    Field('Parallel attempts', ('explore', 'optimization', 'parallel'),
          lambda value: _integer(value, 1),
          comment='Maximum optimization attempts allowed to run concurrently.'),
    Field('Maximum adjustments (0 is unlimited)',
          ('explore', 'optimization', 'max_adjustments'), _integer,
          comment='Reviewer-requested retries per attempt; 0 removes the cap.'),
    Field('Maximum changed files (0 is unlimited)',
          ('explore', 'optimization', 'max_files'), _integer,
          comment='Reject candidates changing more files; 0 removes the cap.'),
    Field('Maximum changed lines (0 is unlimited)',
          ('explore', 'optimization', 'max_lines'), _integer,
          comment='Reject candidates exceeding added plus removed lines.'),
    Field('Protected paths (JSON array)',
          ('explore', 'optimization', 'protected'), _string_list,
          comment='Repository-relative patterns that candidates must not modify.'),
    Field('Inspection command', ('explore', 'inspection', 'command'), _command,
          fallback=('explore', 'command'),
          comment='Agent command that independently reviews completed attempts.'),
    Field('Inspection timeout', ('explore', 'inspection', 'timeout'), _positive,
          fallback=('explore', 'timeout'),
          comment='Maximum runtime for one inspection job.'),
    Field('Validation timeout', ('explore', 'validation', 'timeout'), _positive,
          fallback=('explore', 'timeout'),
          comment='Maximum runtime for each trusted validation job.'),
    Field(
        'Validation GPUs', ('explore', 'validation', 'gpus'), _integer,
        comment='GPUs reserved for each baseline or candidate validation job.'),
    Field('Validation checks (JSON array)',
          ('explore', 'validation', 'checks'), _string_list,
          comment='Commands that must all exit successfully, as a JSON array.'),
    Field('Enable numeric scoring', kind='radio', options=(False, True),
          special='score_enabled',
          comment='Compare candidates with a numeric baseline as well as checks.'),
    Field('Score command', ('explore', 'validation', 'score'), _text,
          visible=_score_enabled,
          comment='Its final non-empty output line must be the numeric score.'),
    Field('Score direction', ('explore', 'validation', 'score_direction'),
          kind='radio', options=('min', 'max'), visible=_score_enabled,
          comment='Choose whether lower or higher scores are better.'),
    Field('Minimum score improvement',
          ('explore', 'validation', 'min_improvement'), _number,
          visible=_score_enabled,
          comment='Required absolute score improvement over the baseline.'),
    Field('Merge command', ('explore', 'merge', 'command'), _command,
          fallback=('explore', 'command'),
          comment='Agent command used for merge review and conflict rebasing.'),
    Field('Merge timeout', ('explore', 'merge', 'timeout'), _positive,
          fallback=('explore', 'timeout'),
          comment='Maximum runtime for one merge review or rebase job.'),
    Field('Maximum accepted attempts (0 is unlimited)',
          ('explore', 'merge', 'max_accepted_attempts'), _integer,
          comment='Candidates integrated during this campaign; 0 removes the cap.'),
)


def validate_generated_document(scaffold, generated):
    """Validate and return wizard-managed values from an agent-edited profile."""
    # Remove every field the wizard owns; everything else must be byte-for-byte
    # equivalent at the data-model level (including metadata and prompt refs).
    baseline = json.loads(json.dumps(scaffold))
    candidate = json.loads(json.dumps(generated))
    managed = [field.path for field in FIELDS if field.path[:1] == ('explore',)]
    managed.extend((
        ('explore', 'validation', 'score'),
        ('explore', 'validation', 'score_direction'),
    ))
    for path in managed:
        delete_value(baseline, path)
        delete_value(candidate, path)
    if baseline != candidate:
        raise ValueError(
            'generated profile changed unknown or non-wizard configuration')

    for field in FIELDS:
        if field.special:
            continue
        if field.path[:1] != ('explore',) or not field.is_visible(generated):
            continue
        value = field.current(generated)
        if field.kind == 'radio':
            if value not in field.options:
                raise ValueError('{} must be one of {}'.format(
                    field.label, ', '.join(map(str, field.options))))
            continue
        raw = field.display(generated)
        if value is None:
            raise ValueError('{} is missing'.format(field.label))
        field.parser(raw)
    score = get_value(generated, ('explore', 'validation', 'score'))
    checks = get_value(generated, ('explore', 'validation', 'checks'), [])
    for command in list(checks) + ([score] if score else []):
        try:
            argv = shlex.split(command)
        except (TypeError, ValueError) as error:
            raise ValueError('invalid validation command: {}'.format(error)) from error
        if not argv:
            raise ValueError('validation commands cannot be empty')
    direction = get_value(
        generated, ('explore', 'validation', 'score_direction'))
    if bool(score) != (direction in {'min', 'max'}):
        raise ValueError(
            'numeric score and score_direction must be configured together')
    return generated


def import_generated_values(target, generated):
    """Copy only values represented by the wizard schema."""
    paths = [field.path for field in FIELDS if field.path[:1] == ('explore',)]
    paths.extend((
        ('explore', 'validation', 'score'),
        ('explore', 'validation', 'score_direction'),
    ))
    for path in paths:
        marker = object()
        value = get_value(generated, path, marker)
        if value is marker:
            delete_value(target, path)
        else:
            set_value(target, path, json.loads(json.dumps(value)))


@dataclass
class _RenderedRow:
    response_line: int
    prompt_line: int = 0
    committed: bool = False
    buffer: str = ''
    cursor: int = 0
    selection: int = 0
    error: str = ''

class ExploreInitWizard:
    def __init__(
        self, store, profile, terminal=None, read_key=None, stream=None,
        backend=None,
    ):
        self.store = store
        self.profile = profile
        self.term = terminal or Terminal()
        self.read_key = read_key or self.term.inkey
        self.stream = stream or sys.stdout
        self.backend = backend
        self._rows = {}
        self._order = []
        self._window = []
        self._cursor_line = 0
        self.new_profile = False

    def run(self, restart_complete=True):
        try:
            self._prepare_generation()
        except WizardAbort:
            print(
                'Initialization paused. Resume with: tq explore init {}'.format(
                    self.profile.name), file=self.stream)
            return 130
        if restart_complete and self.profile.complete:
            self.profile.metadata['complete'] = False
            self.profile.metadata['cursor'] = 0
            self.store.save(self.profile)
        index = self._initial_index(min(self.profile.cursor, len(FIELDS) - 1))
        try:
            with self.term.cbreak(), _paste_context(self.term):
                self._write(_show_cursor(self.term))
                self._write(
                    _dim(
                        self.term,
                        'Up/Down move | Tab next | Enter save | '
                        'Left/Right select | Ctrl-C abort') + '\n' + _prompt(
                            self.term,
                            'tq explore init: {}'.format(self.profile.name))
                    + '\n')
                self._cursor_line = 2
                self._render_form(index)
                self._set_cursor(index)
                while True:
                    self._render_response(index, active=True)
                    key = _key(self.read_key)
                    name, text = getattr(key, 'name', None), str(key)
                    if text == '\x03':
                        raise WizardAbort()
                    if name == 'KEY_UP':
                        previous = self._previous(index)
                        if previous is not None:
                            index = previous
                            self._focus(index)
                        continue
                    if name in {'KEY_DOWN', 'KEY_TAB'} or text == '\t':
                        following = self._next(index)
                        if following is not None:
                            index = following
                            self._focus(index)
                        continue
                    if name == 'KEY_LEFT':
                        self._move_left(index)
                    elif name == 'KEY_RIGHT':
                        self._move_right(index)
                    elif name in {
                        'KEY_BACKSPACE', 'KEY_DELETE_BACKWARD',
                    } or text == '\x7f':
                        self._backspace(index)
                    elif name == 'KEY_DELETE':
                        self._delete(index)
                    elif name == 'KEY_HOME':
                        self._rows[index].cursor = 0
                    elif name == 'KEY_END':
                        self._rows[index].cursor = len(
                            self._rows[index].buffer)
                    elif name == 'KEY_ENTER' or text in {'\n', '\r'}:
                        if not self._commit(index):
                            continue
                        following = self._next(index)
                        if following is None:
                            break
                        index = following
                        self._focus(index)
                    else:
                        inserted = _input_text(key)
                        if inserted:
                            self._insert(index, inserted)
                self.profile.metadata['complete'] = True
                self.profile.metadata['cursor'] = len(FIELDS)
                self.store.save(self.profile)
                self._write('\n')
        except WizardAbort:
            self._write('\n')
            print(
                'Initialization paused. Resume with: tq explore init {}'.format(
                    self.profile.name), file=self.stream)
            return 130
        return 0

    def _prepare_generation(self):
        initialization = self.store.resolved_config.get(
            'explore', {}).get('initialization')
        if not initialization or not initialization.get('command'):
            return
        generation = self.store.read_generation(self.profile)
        if self.new_profile:
            generation = self.store.save_generation(
                self.profile, status='draft', attempts=0,
                objective_prompt=generation.get('objective_prompt'))

        status = generation.get('status')
        legacy_pending = (
            status == 'pending' and
            not generation.get('run_token') and
            generation.get('backend_job_id') is None
        )
        editable = status in {'draft', 'ready', 'interrupted'} or legacy_pending
        active = (
            status in {'queued', 'running', 'pending'} and
            bool(generation.get('run_token') or
                 generation.get('backend_job_id') is not None)
        )
        from .initialization import (
            InitializationInterrupted,
            ProfileInitializationJob,
        )
        if active:
            try:
                generation = ProfileInitializationJob(
                    self.store, self.profile, initialization, self.backend,
                    objective_prompt=generation.get('objective_prompt'),
                    stream=self.stream).reconcile_interrupted()
            except InitializationInterrupted as error:
                raise WizardAbort() from error
            status = generation.get('status')
            editable = status in {'draft', 'ready', 'interrupted'}
            active = (
                status in {'queued', 'running', 'pending'} and
                bool(generation.get('run_token') or
                     generation.get('backend_job_id') is not None)
            )
        if not editable and not active:
            return

        objective_prompt = generation.get('objective_prompt')
        if editable:
            objective_prompt = prompt_objective(
                objective_prompt or self.profile.objective, terminal=self.term,
                read_key=self.read_key, stream=self.stream)
            generation = self.store.save_generation(
                self.profile,
                expected_run_token=generation.get('run_token'),
                status='ready',
                objective_prompt=objective_prompt,
                run_token=None,
                backend_job_id=None,
            )
            if generation is None:
                print(
                    'Profile generation request changed while editing the brief.',
                    file=self.stream)
                raise WizardAbort()
        try:
            ProfileInitializationJob(
                self.store, self.profile, initialization, self.backend,
                objective_prompt=objective_prompt,
                stream=self.stream).run()
        except InitializationInterrupted as error:
            raise WizardAbort() from error
        self.profile = self.store.load(self.profile.name)

    def _write(self, value):
        print(value, end='', file=self.stream)
        self.stream.flush()

    def _goto(self, line):
        self._write(_move_lines(self.term, line - self._cursor_line) + '\r')
        self._cursor_line = line

    def _visible_indices(self):
        return [
            index for index, field in enumerate(FIELDS)
            if field.is_visible(self.profile.document)
        ]

    def _initial_index(self, index):
        for candidate in self._visible_indices():
            if candidate >= index:
                return candidate
        return self._visible_indices()[-1]

    def _new_row(self, index, response_line, committed):
        field = FIELDS[index]
        current = self._field_current(index)
        selection = 0
        if field.kind == 'radio' and current in field.options:
            selection = field.options.index(current)
        buffer = (
            self._field_display(index)
            if committed and field.kind != 'radio' else '')
        return _RenderedRow(
            response_line=response_line,
            committed=committed,
            buffer=buffer,
            cursor=len(buffer),
            selection=selection,
        )

    def _field_current(self, index):
        field = FIELDS[index]
        if field.special == 'objective':
            return self.profile.objective
        return field.current(self.profile.document)

    def _field_display(self, index):
        field = FIELDS[index]
        if field.special == 'objective':
            return self.profile.objective
        return field.display(self.profile.document)

    def _render_form(self, active_index):
        self._order = self._visible_indices()
        self._rows = {
            index: self._new_row(index, 0, index < active_index)
            for index in self._order
        }
        self._render_window(active_index)

    def _rebuild_form(self, active_index):
        old_rows = self._rows
        self._order = self._visible_indices()
        self._rows = {
            index: old_rows.get(index) or self._new_row(
                index, 0, index < active_index)
            for index in self._order
        }
        self._render_window(active_index)

    def _window_capacity(self):
        height = int(getattr(self.term, 'height', 24) or 24)
        row_height = max(
            2 + len(FIELDS[index].comment_lines)
            for index in self._order)
        return max(1, min(
            len(self._order), (height - 3) // row_height))

    def _render_window(self, active_index):
        capacity = self._window_capacity()
        position = self._order.index(active_index)
        start = max(0, min(
            position - capacity // 2, len(self._order) - capacity))
        self._window = self._order[start:start + capacity]
        self._goto(2)
        self._write(_clear_eos(self.term))
        self._cursor_line = 2
        for index in self._window:
            row = self._rows[index]
            row.prompt_line = self._cursor_line
            row.response_line = (
                row.prompt_line + 1 + len(FIELDS[index].comment_lines))
            self._render_prompt(index)
            self._render_response(index)
            self._write('\n')
            self._cursor_line = row.response_line + 1
        self._goto(self._rows[active_index].response_line)

    def _focus(self, index):
        if index not in self._window:
            self._render_window(index)
        self._set_cursor(index)

    def _set_cursor(self, index):
        self.profile.metadata['cursor'] = index
        self.store.save(self.profile)

    def _next(self, index):
        try:
            position = self._order.index(index) + 1
        except ValueError:
            return None
        return self._order[position] if position < len(self._order) else None

    def _previous(self, index):
        try:
            position = self._order.index(index) - 1
        except ValueError:
            return None
        return self._order[position] if position >= 0 else None

    def _commit(self, index):
        field = FIELDS[index]
        row = self._rows[index]
        raw = row.buffer or self._field_display(index)
        try:
            if field.kind == 'radio':
                field.commit(
                    self.profile.document, '', field.options[row.selection])
            elif field.special == 'objective':
                self.store.save_objective(self.profile, field.parser(raw))
            else:
                field.commit(self.profile.document, raw)
        except (
            BackendError, TypeError, ValueError, json.JSONDecodeError,
        ) as exception:
            row.error = str(exception)
            self._render_response(index, active=True)
            return False
        row.committed = True
        row.error = ''
        if field.kind != 'radio':
            row.buffer = self._field_display(index)
            row.cursor = len(row.buffer)
        self.store.save(self.profile)
        if self._visible_indices() != self._order:
            self._rebuild_form(index)
        else:
            self._render_response(index)
        return True

    def _move_left(self, index):
        row = self._rows[index]
        if FIELDS[index].kind == 'radio':
            row.selection = (row.selection - 1) % len(FIELDS[index].options)
        else:
            row.cursor = max(0, row.cursor - 1)

    def _move_right(self, index):
        row = self._rows[index]
        if FIELDS[index].kind == 'radio':
            row.selection = (row.selection + 1) % len(FIELDS[index].options)
        else:
            row.cursor = min(len(row.buffer), row.cursor + 1)

    def _backspace(self, index):
        row = self._rows[index]
        if FIELDS[index].kind == 'radio' or not row.cursor:
            return
        row.buffer = row.buffer[:row.cursor - 1] + row.buffer[row.cursor:]
        row.cursor -= 1

    def _delete(self, index):
        row = self._rows[index]
        if FIELDS[index].kind != 'radio':
            row.buffer = row.buffer[:row.cursor] + row.buffer[row.cursor + 1:]

    def _insert(self, index, text):
        row = self._rows[index]
        if FIELDS[index].kind != 'radio':
            row.buffer = row.buffer[:row.cursor] + text + row.buffer[row.cursor:]
            row.cursor += len(text)

    def _render_prompt(self, index):
        row = self._rows[index]
        field = FIELDS[index]
        self._goto(row.prompt_line)
        self._write(_clear_line(self.term) + _prompt(self.term, field.label))
        for comment in field.comment_lines:
            self._write('\n')
            self._cursor_line += 1
            self._write(_clear_line(self.term) + _comment(self.term, comment))
        self._write('\n')
        self._cursor_line = row.response_line

    def _render_response(self, index, active=False):
        row = self._rows[index]
        field = FIELDS[index]
        if field.kind == 'radio':
            choices = []
            for offset, value in enumerate(field.options):
                label = (
                    'Enabled' if value is True else
                    'Disabled' if value is False else str(value))
                if offset == row.selection:
                    rendered = _selected(self.term, '◉ {}'.format(label))
                else:
                    rendered = '○ {}'.format(label)
                choices.append(rendered)
            line = '  '.join(choices)
            if row.error:
                line += '  ' + _red(self.term, 'Error: ' + row.error)
        else:
            placeholder = self._field_display(index)
        self._goto(row.response_line)
        if field.kind == 'radio':
            self._write(_clear_line(self.term) + RESPONSE_PREFIX + line)
        else:
            line, column = _render_text_line(
                self.term, row.buffer, row.cursor,
                placeholder=placeholder, error=row.error)
            self._write(line)
        if active and field.kind != 'radio':
            self._write(_move_column(self.term, column))


def choose_profile(store, terminal=None, read_key=None, stream=None, create=True):
    term = terminal or Terminal()
    read_key = read_key or term.inkey
    stream = stream or sys.stdout
    options = store.list() + (['Create new'] if create else [])
    if not options:
        return prompt_name(
            terminal=term, read_key=read_key, stream=stream,
            validator=store.validate_name)
    selected = 0
    with term.cbreak():
        print(_show_cursor(term), end='', file=stream)
        print(_prompt(term, 'Choose exploration profile'), file=stream)
        print(_comment(
            term, 'Select an existing profile or create a new one.'),
            file=stream)
        while True:
            rendered = []
            for index, value in enumerate(options):
                if index == selected:
                    label = _selected(term, '◉ {}'.format(value))
                else:
                    label = '○ {}'.format(value)
                rendered.append(label)
            print(
                _clear_line(term) + RESPONSE_PREFIX + '  '.join(rendered),
                end='', file=stream)
            stream.flush()
            key = _inline_key(read_key, stream)
            name, text = getattr(key, 'name', None), str(key)
            if name == 'KEY_LEFT':
                selected = (selected - 1) % len(options)
            elif name == 'KEY_RIGHT':
                selected = (selected + 1) % len(options)
            elif name == 'KEY_ENTER' or text in {'\n', '\r'}:
                choice = options[selected]
                break
        print(file=stream)
    if choice == 'Create new':
        return prompt_name(
            terminal=term, read_key=read_key, stream=stream,
            validator=store.validate_name)
    return choice


def prompt_name(terminal=None, read_key=None, stream=None, validator=None):
    term = terminal or Terminal()
    read_key = read_key or term.inkey
    stream = stream or sys.stdout
    value = ''
    error = ''
    with term.cbreak():
        print(_show_cursor(term), end='', file=stream)
        print(_prompt(term, 'New exploration profile name'), file=stream)
        print(_comment(
            term, 'Used as the directory name under .tq/explore/.'),
            file=stream)
        while True:
            shown = value or _dim(term, 'default')
            message = _clear_line(term) + RESPONSE_PREFIX + shown
            if error:
                message += '  ' + _red(term, 'Error: ' + error)
            print(message, end='', file=stream)
            stream.flush()
            key = _inline_key(read_key, stream)
            name, text = getattr(key, 'name', None), str(key)
            if name in {'KEY_BACKSPACE', 'KEY_DELETE_BACKWARD'} or text == '\x7f':
                value = value[:-1]
            elif name == 'KEY_ENTER' or text in {'\n', '\r'}:
                candidate = value or 'default'
                try:
                    result = validator(candidate) if validator else candidate
                    print(file=stream)
                    return result
                except (TypeError, ValueError, BackendError) as exception:
                    error = str(exception)
            elif text.isprintable() and not name:
                value += text


def prompt_objective(default='', terminal=None, read_key=None, stream=None):
    term = terminal or Terminal()
    read_key = read_key or term.inkey
    stream = stream or sys.stdout
    value, error = '', ''
    with term.cbreak(), _paste_context(term):
        print(_show_cursor(term), end='', file=stream)
        print(_prompt(term, 'Profile generation brief'), file=stream)
        print(_comment(
            term, 'Describe what the setup agent should configure for this repository.'),
            file=stream)
        while True:
            message, column = _render_text_line(
                term, value, len(value), placeholder=default, error=error)
            print(message + _move_column(term, column), end='', file=stream)
            stream.flush()
            key = _inline_key(read_key, stream)
            name, text = getattr(key, 'name', None), str(key)
            if name in {'KEY_BACKSPACE', 'KEY_DELETE_BACKWARD'} or text == '\x7f':
                value = value[:-1]
            elif name == 'KEY_ENTER' or text in {'\n', '\r'}:
                try:
                    result = _text(value or default)
                    print(file=stream)
                    return result
                except ValueError as exception:
                    error = str(exception)
            else:
                value += _input_text(key)


def confirm_remove(name, summary, terminal=None, read_key=None, stream=None):
    term = terminal or Terminal()
    read_key = read_key or term.inkey
    stream = stream or sys.stdout
    selected = 0
    options = ('Cancel', 'Remove')
    with term.cbreak():
        print(_show_cursor(term), end='', file=stream)
        print(_prompt(term, 'Remove profile {}?'.format(name)), file=stream)
        for line in summary.splitlines():
            print(_comment(term, line), file=stream)
        while True:
            rendered = []
            for index, value in enumerate(options):
                if index == selected:
                    label = _selected(term, '◉ {}'.format(value))
                else:
                    label = '○ {}'.format(value)
                rendered.append(label)
            print(
                _clear_line(term) + RESPONSE_PREFIX + '  '.join(rendered),
                end='', file=stream)
            stream.flush()
            key = _inline_key(read_key, stream)
            key_name, text = getattr(key, 'name', None), str(key)
            if key_name == 'KEY_LEFT':
                selected = (selected - 1) % 2
            elif key_name == 'KEY_RIGHT':
                selected = (selected + 1) % 2
            elif key_name == 'KEY_ENTER' or text in {'\n', '\r'}:
                print(file=stream)
                return selected == 1

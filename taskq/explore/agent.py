"""Agent commands, prompts, and structured response validation."""

import hashlib
import json
import re
import shlex
import unicodedata
from collections.abc import Mapping, Sequence
from string import Template
from typing import Any, Dict, List, Optional, Union


DEFAULT_COMMAND_TEMPLATE = ['codex', 'exec', '{}']
REVIEW_DECISIONS = frozenset({
    'accept',
    'adjust',
    'abandon',
    'stop',
})
TASKQ_JSON_PREFIX = 'TASKQ_JSON:'


class AgentResponseError(ValueError):
    """Raised when an agent's structured response is invalid."""


def parse_command_template(
    command: Union[str, Sequence[str]] = DEFAULT_COMMAND_TEMPLATE,
) -> List[str]:
    """Parse an argv template and require one standalone prompt token."""
    if isinstance(command, str):
        try:
            argv = shlex.split(command, posix=True)
        except ValueError as exc:
            raise ValueError('invalid agent command template: {}'.format(exc))
    elif isinstance(command, Sequence):
        argv = list(command)
    else:
        raise TypeError('agent command template must be text or an argv sequence')

    if not argv or any(not isinstance(arg, str) for arg in argv):
        raise ValueError('agent command template must contain string arguments')
    if any('\0' in arg for arg in argv):
        raise ValueError('agent command template cannot contain NUL bytes')
    if argv.count('{}') != 1:
        raise ValueError(
            'agent command template must contain exactly one standalone {} token'
        )
    return argv


def render_command(
    command: Union[str, Sequence[str]], prompt: str,
) -> List[str]:
    """Return argv with the prompt inserted as one argument; no shell is used."""
    if not isinstance(prompt, str):
        raise TypeError('agent prompt must be text')
    return [prompt if arg == '{}' else arg for arg in parse_command_template(command)]


def build_planner_prompt(
    objective: str,
    *,
    template: str,
    memory: Any = None,
    tried_directions: Any = None,
    direction_count: int = 1,
    max_files: int = 5,
    max_lines: int = 300,
    **context: Any
) -> str:
    """Build a prompt for a non-mutating direction-planning turn."""
    if direction_count < 1:
        raise ValueError('direction_count must be positive')
    return render_prompt(
        template,
        objective=objective,
        memory=memory,
        tried_directions=tried_directions,
        context=context,
        direction_count=direction_count,
        change_scope=_change_scope(max_files, max_lines) or 'unlimited',
    )


def build_optimizer_prompt(
    objective: str,
    direction: Any,
    *,
    template: str,
    adjust_template: Optional[str] = None,
    memory: Any = None,
    artifacts: Any = None,
    adjust: bool = False,
    max_files: int = 5,
    max_lines: int = 300,
    **context: Any
) -> str:
    """Build a worktree mutation prompt for an initial or follow-up action."""
    selected = adjust_template if adjust else template
    if selected is None:
        raise ValueError('adjustment prompt template is required')
    return render_prompt(
        selected,
        objective=objective,
        direction=direction,
        memory=memory,
        artifacts=artifacts,
        context=context,
        change_scope=_change_scope(max_files, max_lines) or 'unlimited',
    )


def build_reviewer_prompt(
    objective: str,
    direction: Any,
    *,
    template: str,
    memory: Any = None,
    artifacts: Any = None,
    max_files: int = 5,
    max_lines: int = 300,
    **context: Any
) -> str:
    """Build a non-mutating prompt for an independent attempt review."""
    return render_prompt(
        template,
        objective=objective,
        direction=direction,
        memory=memory,
        artifacts=artifacts,
        context=context,
        change_scope=_change_scope(max_files, max_lines) or 'unlimited',
    )


def build_rebase_prompt(
    objective: str,
    direction: Any,
    *,
    template: str,
    memory: Any = None,
    artifacts: Any = None,
    max_files: int = 5,
    max_lines: int = 300,
    **context: Any
) -> str:
    """Build a worktree prompt for resolving a rebase conflict."""
    return render_prompt(
        template,
        objective=objective,
        direction=direction,
        memory=memory,
        artifacts=artifacts,
        context=context,
        change_scope=_change_scope(max_files, max_lines) or 'unlimited',
    )


def render_prompt(template: str, **values: Any) -> str:
    """Render a TOML-owned prompt template with serialized context values."""
    if not _nonempty_text(template):
        raise ValueError('prompt template must be non-empty text')
    if 'objective' in values and not _nonempty_text(values['objective']):
        raise ValueError('objective must be non-empty text')
    formatted = {key: _format_value(value) for key, value in values.items()}
    try:
        return Template(template).substitute(formatted).strip()
    except KeyError as error:
        raise ValueError(
            'prompt template references unknown value ${}'.format(error.args[0])
        ) from error
    except ValueError as error:
        raise ValueError('invalid prompt template: {}'.format(error)) from error


def extract_taskq_json(output: str) -> Dict[str, Any]:
    """Extract a JSON object from the final non-empty response line."""
    if not isinstance(output, str):
        raise AgentResponseError('agent response must be text')
    lines = output.rstrip().splitlines()
    if not lines:
        raise AgentResponseError('agent response is empty')
    line = lines[-1].lstrip()
    if not line.startswith(TASKQ_JSON_PREFIX):
        raise AgentResponseError(
            'agent response must end with a single-line TASKQ_JSON object'
        )
    payload = line[len(TASKQ_JSON_PREFIX):].strip()
    if not payload:
        raise AgentResponseError('TASKQ_JSON payload is empty')
    try:
        value = json.loads(payload)
    except (TypeError, ValueError) as exc:
        raise AgentResponseError('invalid TASKQ_JSON: {}'.format(exc))
    if not isinstance(value, dict):
        raise AgentResponseError('TASKQ_JSON payload must be an object')
    return value


def parse_planner_response(
    output: str, expected_count: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Validate and return planned direction objects."""
    value = extract_taskq_json(output)
    directions = value.get('directions')
    if not isinstance(directions, list) or not directions:
        raise AgentResponseError('planner response requires a non-empty directions list')
    if expected_count is not None and len(directions) != expected_count:
        raise AgentResponseError(
            'planner returned {} directions; expected {}'.format(
                len(directions), expected_count
            )
        )

    for index, direction in enumerate(directions):
        if not isinstance(direction, dict) or not _direction_has_text(direction):
            raise AgentResponseError(
                'direction {} must be an object with descriptive text'.format(index)
            )
        different_from = direction.get('different_from')
        if different_from is not None and not isinstance(different_from, list):
            raise AgentResponseError(
                'direction {} different_from must be a list'.format(index)
            )
    return directions


def parse_reviewer_response(output: str) -> Dict[str, Any]:
    """Validate and return a reviewer decision object."""
    value = extract_taskq_json(output)
    decision = value.get('decision')
    if decision not in REVIEW_DECISIONS:
        raise AgentResponseError(
            'reviewer decision must be one of {}'.format(
                ', '.join(sorted(REVIEW_DECISIONS))
            )
        )
    if not _nonempty_text(value.get('reason')):
        raise AgentResponseError('reviewer decision requires a reason')

    for key in ('evidence', 'memory_updates'):
        if key in value and not isinstance(value[key], list):
            raise AgentResponseError('{} must be a list'.format(key))
    next_direction = value.get('next_direction')
    if next_direction is not None:
        if not isinstance(next_direction, dict) or not _direction_has_text(next_direction):
            raise AgentResponseError(
                'next_direction must be null or an object with descriptive text'
            )

    value.setdefault('evidence', [])
    value.setdefault('memory_updates', [])
    value.setdefault('next_direction', None)
    return value


_QUESTION_PATTERNS = tuple(re.compile(pattern, re.IGNORECASE) for pattern in (
    r'\?\s*$',
    r'\b(?:please|could you|can you)\s+(?:provide|clarify|confirm|choose|tell me)',
    r'\b(?:need|require)\s+(?:your|user)\s+(?:input|clarification|confirmation)',
    r'\b(?:waiting|awaiting)\s+(?:for\s+)?(?:your|user)\s+',
    r'\bwould you like me to\b',
))
_STALL_PATTERNS = tuple(re.compile(pattern, re.IGNORECASE) for pattern in (
    r'\b(?:can(?:not|\'t)|unable to)\s+(?:proceed|continue|complete)\b',
    r'\bblocked\s+by\b',
    r'\bno changes (?:were |have been )?made\b',
    r'\bdid not make (?:any )?changes\b',
    r'\b(?:waiting|awaiting)\s+(?:input|clarification|confirmation)\b',
))


def is_question_like(output: str) -> bool:
    """Return whether an agent response ends by requesting interaction."""
    if not isinstance(output, str):
        return False
    tail = '\n'.join(output.strip().splitlines()[-3:])
    return bool(tail and any(pattern.search(tail) for pattern in _QUESTION_PATTERNS))


def is_stalled_output(output: str, changed: Optional[bool] = None) -> bool:
    """Detect empty, interactive, explicitly blocked, or no-change responses."""
    if changed is False or not isinstance(output, str) or not output.strip():
        return True
    return is_question_like(output) or any(
        pattern.search(output) for pattern in _STALL_PATTERNS
    )


_FINGERPRINT_KEYS = ('hypothesis', 'approach', 'prompt', 'direction')
_FINGERPRINT_STOPWORDS = frozenset({'a', 'an', 'and', 'for', 'in', 'of', 'on', 'the', 'to'})


def fingerprint_direction(direction: Any) -> str:
    """Return a stable content fingerprint for duplicate-direction detection."""
    if isinstance(direction, Mapping):
        parts = [direction[key] for key in _FINGERPRINT_KEYS if key in direction]
        if not parts:
            parts = [direction.get('title', direction)]
        text = ' '.join(_format_value(part) for part in parts)
    else:
        text = _format_value(direction)
    normalized = unicodedata.normalize('NFKC', text).casefold()
    tokens = [
        token for token in re.findall(r'\w+', normalized, flags=re.UNICODE)
        if token not in _FINGERPRINT_STOPWORDS
    ]
    canonical = ' '.join(tokens)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _format_value(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def _direction_has_text(direction: Mapping) -> bool:
    keys = ('title', 'hypothesis', 'approach', 'prompt', 'direction')
    return any(_nonempty_text(direction.get(key)) for key in keys)


def _nonempty_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_limits(max_files: int, max_lines: int) -> None:
    if max_files < 0 or max_lines < 0:
        raise ValueError('change limits cannot be negative')


def _change_scope(max_files: int, max_lines: int) -> str:
    _validate_limits(max_files, max_lines)
    limits = []
    if max_files:
        limits.append('{} file{}'.format(
            max_files, '' if max_files == 1 else 's'))
    if max_lines:
        limits.append('{} line{}'.format(
            max_lines, '' if max_lines == 1 else 's'))
    return ' and '.join(limits)

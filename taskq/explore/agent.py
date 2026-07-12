"""Agent commands, prompts, and structured response validation."""

import hashlib
import json
import re
import shlex
import unicodedata
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Union


DEFAULT_COMMAND_TEMPLATE = ['codex', 'exec', '{}']
REVIEW_DECISIONS = frozenset({
    'accept',
    'adjust',
    'abandon',
    'evaluate_more',
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
    memory: Any = None,
    tried_directions: Any = None,
    direction_count: int = 1,
    max_files: int = 5,
    max_lines: int = 300,
    **context: Any
) -> str:
    """Build a prompt for a non-mutating direction-planning turn."""
    _validate_limits(max_files, max_lines)
    if direction_count < 1:
        raise ValueError('direction_count must be positive')
    return _prompt(
        'Plan {} distinct optimization work direction(s).'.format(direction_count),
        objective,
        memory=memory,
        tried_directions=tried_directions,
        context=context,
        rules=[
            'Do not edit the repository; propose bounded work only.',
            'Each direction must fit within {} changed files and {} changed lines.'.format(
                max_files, max_lines
            ),
            'Use prior evidence and make every direction structurally distinct from '
            'recorded successes, failures, and the other proposed directions.',
            'Do not ask the user questions; make the best safe assumption.',
            'Return exactly {} direction(s).'.format(direction_count),
            'End with one single-line JSON object in this exact shape: '
            'TASKQ_JSON: {"directions":[{"title":"...","hypothesis":"...",'
            '"approach":"...","success_signal":"...",'
            '"different_from":["..."]}]}',
        ],
    )


def build_optimizer_prompt(
    objective: str,
    direction: Any,
    *,
    memory: Any = None,
    artifacts: Any = None,
    adjust: bool = False,
    max_files: int = 5,
    max_lines: int = 300,
    **context: Any
) -> str:
    """Build a worktree mutation prompt for an initial or follow-up action."""
    _validate_limits(max_files, max_lines)
    action = (
        'Improve the existing attempt according to the review evidence.'
        if adjust else
        'Implement one small optimization attempt in the current worktree.'
    )
    return _prompt(
        action,
        objective,
        direction=direction,
        memory=memory,
        artifacts=artifacts,
        context=context,
        rules=[
            'Work only in the current worktree and leave the repository in a '
            'reviewable state.',
            'Change at most {} files and {} lines; keep the action focused.'.format(
                max_files, max_lines
            ),
            'Do not weaken tests, benchmarks, evaluators, fixtures, or campaign state.',
            'Inspect and test what you can, but do not wait for or ask for user input.',
            'If the exact approach is blocked, make the safest useful bounded progress '
            'and explain the evidence in your final response.',
        ],
    )


def build_reviewer_prompt(
    objective: str,
    direction: Any,
    *,
    memory: Any = None,
    artifacts: Any = None,
    max_files: int = 5,
    max_lines: int = 300,
    **context: Any
) -> str:
    """Build a non-mutating prompt for an independent attempt review."""
    _validate_limits(max_files, max_lines)
    return _prompt(
        'Independently inspect the completed optimization action.',
        objective,
        direction=direction,
        memory=memory,
        artifacts=artifacts,
        context=context,
        rules=[
            'Treat worker claims as untrusted; base the decision on the diff, Git '
            'state, logs, and trusted evaluator artifacts.',
            'Never accept changes that exceed {} files or {} lines, alter protected '
            'inputs, fail a required check, or miss a required score threshold.'.format(
                max_files, max_lines
            ),
            'Choose exactly one decision: accept, adjust, abandon, evaluate_more, or '
            'stop. Use adjust only for a promising bounded follow-up.',
            'Do not ask the user questions and do not edit the repository.',
            'Any next direction must be distinct from recorded directions.',
            'End with one single-line JSON object in this exact shape: '
            'TASKQ_JSON: {"decision":"adjust","reason":"...",'
            '"evidence":["artifact-id"],"memory_updates":[],'
            '"next_direction":null}',
        ],
    )


def build_rebase_prompt(
    objective: str,
    direction: Any,
    *,
    memory: Any = None,
    artifacts: Any = None,
    max_files: int = 5,
    max_lines: int = 300,
    **context: Any
) -> str:
    """Build a worktree prompt for resolving a rebase conflict."""
    _validate_limits(max_files, max_lines)
    return _prompt(
        'Resolve the in-progress rebase in the current attempt worktree.',
        objective,
        direction=direction,
        memory=memory,
        artifacts=artifacts,
        context=context,
        rules=[
            'Inspect Git status and resolve only the current conflicts, preserving the '
            'target branch behavior and the useful intent of this optimization.',
            'Complete the existing rebase; do not abort it or create a merge commit.',
            'Keep resolution changes within {} files and {} lines unless the existing '
            'conflict itself already exceeds that boundary.'.format(max_files, max_lines),
            'Do not weaken tests, benchmarks, evaluators, fixtures, or campaign state.',
            'Do not ask the user questions; make the safest resolution supported by '
            'the repository and artifacts.',
        ],
    )


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


def _prompt(
    role: str,
    objective: str,
    *,
    direction: Any = None,
    memory: Any = None,
    tried_directions: Any = None,
    artifacts: Any = None,
    context: Optional[Dict[str, Any]] = None,
    rules: Sequence[str]
) -> str:
    if not _nonempty_text(objective):
        raise ValueError('objective must be non-empty text')
    sections = [role, '', 'Objective:', objective.strip()]
    for title, value in (
        ('Direction', direction),
        ('Relevant memory', memory),
        ('Previously tried directions', tried_directions),
        ('Artifacts and evidence', artifacts),
        ('Additional context', context),
    ):
        if value not in (None, {}, [], ()):
            sections.extend(('', '{}:'.format(title), _format_value(value)))
    sections.extend(('', 'Rules:'))
    sections.extend('- ' + rule for rule in rules)
    return '\n'.join(sections)


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
    if max_files < 1 or max_lines < 1:
        raise ValueError('change limits must be positive')

import pytest

from taskq.explore.agent import (
    DEFAULT_COMMAND_TEMPLATE,
    REVIEW_DECISIONS,
    AgentResponseError,
    build_optimizer_prompt,
    build_planner_prompt,
    build_rebase_prompt,
    build_reviewer_prompt,
    extract_taskq_json,
    fingerprint_direction,
    is_question_like,
    is_stalled_output,
    parse_command_template,
    parse_planner_response,
    parse_reviewer_response,
    render_command,
)


def test_command_template_parses_and_renders_prompt_as_one_argument():
    template = parse_command_template('other-agent run --model "small model" "{}"')
    prompt = 'optimize this; touch nothing else\nsecond line'

    assert template == ['other-agent', 'run', '--model', 'small model', '{}']
    assert render_command(template, prompt) == [
        'other-agent', 'run', '--model', 'small model', prompt,
    ]
    assert DEFAULT_COMMAND_TEMPLATE == ['codex', 'exec', '{}']


@pytest.mark.parametrize('command', [
    '',
    'codex exec',
    'codex exec --prompt={}',
    'codex exec {} {}',
    ['codex', 'exec', 1, '{}'],
])
def test_command_template_requires_one_standalone_placeholder(command):
    with pytest.raises((TypeError, ValueError)):
        parse_command_template(command)


def test_prompt_builders_include_role_context_and_safety_contracts():
    planner = build_planner_prompt(
        'make polling faster',
        memory=[{'claim': 'cache helped'}],
        tried_directions=['cache metadata'],
        direction_count=2,
    )
    optimizer = build_optimizer_prompt(
        'make polling faster',
        {'hypothesis': 'batch stat calls'},
        artifacts={'review': 'promising'},
        adjust=True,
    )
    reviewer = build_reviewer_prompt(
        'make polling faster',
        'batch stat calls',
        artifacts={'diff': 'artifact-2'},
    )
    rebase = build_rebase_prompt(
        'make polling faster',
        'batch stat calls',
        artifacts='conflicts: broker.py',
    )

    assert 'exactly 2 direction(s)' in planner
    assert 'structurally distinct' in planner
    assert 'TASKQ_JSON:' in planner
    assert 'Improve the existing attempt' in optimizer
    assert 'current worktree' in optimizer
    assert '5 files and 300 lines' in optimizer
    assert 'worker claims as untrusted' in reviewer
    assert ', '.join(sorted(REVIEW_DECISIONS)) not in reviewer
    assert 'accept, adjust, abandon, evaluate_more, or stop' in reviewer
    assert 'in-progress rebase' in rebase
    assert 'do not abort it or create a merge commit' in rebase
    for prompt in (planner, optimizer, reviewer, rebase):
        assert 'Do not ask the user questions' in prompt or 'do not wait for or ask' in prompt


def test_extract_taskq_json_requires_single_final_marker_line():
    assert extract_taskq_json(
        'Evidence above.\nTASKQ_JSON: {"decision":"accept"}\n'
    ) == {'decision': 'accept'}

    with pytest.raises(AgentResponseError):
        extract_taskq_json('TASKQ_JSON:\n{"decision":"accept"}')
    with pytest.raises(AgentResponseError):
        extract_taskq_json('TASKQ_JSON: []')
    with pytest.raises(AgentResponseError):
        extract_taskq_json('TASKQ_JSON: {}\ntrailing prose')


def test_parse_planner_response_validates_directions_and_count():
    output = (
        'Plan ready.\nTASKQ_JSON: {"directions":['
        '{"title":"Batch reads","hypothesis":"fewer syscalls"},'
        '{"prompt":"Cache immutable metadata","different_from":["Batch reads"]}'
        ']}'
    )
    directions = parse_planner_response(output, expected_count=2)

    assert directions[0]['title'] == 'Batch reads'
    assert directions[1]['different_from'] == ['Batch reads']
    with pytest.raises(AgentResponseError):
        parse_planner_response(output, expected_count=1)
    with pytest.raises(AgentResponseError):
        parse_planner_response('TASKQ_JSON: {"directions":[{}]}')


@pytest.mark.parametrize('decision', sorted(REVIEW_DECISIONS))
def test_parse_reviewer_response_accepts_all_decisions(decision):
    output = (
        'TASKQ_JSON: {"decision":"%s","reason":"evidence supports it",'
        '"next_direction":{"hypothesis":"try batching"}}' % decision
    )

    value = parse_reviewer_response(output)

    assert value['decision'] == decision
    assert value['evidence'] == []
    assert value['memory_updates'] == []
    assert value['next_direction']['hypothesis'] == 'try batching'


def test_parse_reviewer_response_rejects_bad_decision_shape():
    with pytest.raises(AgentResponseError):
        parse_reviewer_response(
            'TASKQ_JSON: {"decision":"merge","reason":"looks good"}'
        )
    with pytest.raises(AgentResponseError):
        parse_reviewer_response('TASKQ_JSON: {"decision":"accept"}')
    with pytest.raises(AgentResponseError):
        parse_reviewer_response(
            'TASKQ_JSON: {"decision":"accept","reason":"good",'
            '"next_direction":[]}'
        )


def test_question_and_stall_detection():
    assert is_question_like('Work is ready. Would you like me to run tests?')
    assert is_stalled_output('I cannot proceed without credentials.')
    assert is_stalled_output('Implemented a change.', changed=False)
    assert is_stalled_output('')
    assert not is_question_like('Resolved the question in the implementation.')
    assert not is_stalled_output('Implemented and tested the focused change.', changed=True)


def test_direction_fingerprint_normalizes_surface_form_and_ignores_title():
    first = {
        'title': 'First title',
        'hypothesis': 'Batch the metadata reads!',
        'approach': 'Use one call for each polling cycle.',
    }
    same = {
        'approach': 'use one call for each polling cycle',
        'hypothesis': 'batch metadata reads',
        'title': 'A different label',
    }
    different = {'hypothesis': 'Cache parsed metadata between cycles'}

    assert fingerprint_direction(first) == fingerprint_direction(same)
    assert fingerprint_direction(first) != fingerprint_direction(different)

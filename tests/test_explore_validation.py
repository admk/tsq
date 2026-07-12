import json
import subprocess
import sys

import pytest

from taskq.explore import validation


@pytest.mark.parametrize(
    'command, expected',
    [
        ('runner --label "two words"', ['runner', '--label', 'two words']),
        (['runner', '--label', 'two words'], ['runner', '--label', 'two words']),
    ],
)
def test_run_command_uses_argv_without_a_shell(monkeypatch, command, expected):
    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, 'out', 'err')

    monkeypatch.setattr(validation.subprocess, 'run', fake_run)

    result = validation.run_command(command)

    assert calls[0][0] == expected
    assert 'shell' not in calls[0][1]
    assert result['command'] == expected


def test_run_command_captures_stdout_stderr_and_exit_code():
    result = validation.run_command([
        sys.executable,
        '-c',
        'import sys; print("output"); print("problem", file=sys.stderr); sys.exit(7)',
    ])

    assert result['exit_code'] == 7
    assert result['stdout'] == 'output\n'
    assert result['stderr'] == 'problem\n'


def test_parse_score_uses_last_nonempty_stdout_line():
    assert validation.parse_score('warmup\n\n  12.75  \n') == 12.75

    with pytest.raises(ValueError, match='no output'):
        validation.parse_score(' \n\t\n')
    with pytest.raises(ValueError):
        validation.parse_score('context\nnot-a-number\n')
    for value in ('nan', 'inf', '-inf'):
        with pytest.raises(ValueError, match='finite'):
            validation.parse_score(value)


def test_validate_scores_three_runs_and_uses_the_median(monkeypatch):
    outputs = iter(['9\n', '3\n', '6\n'])

    def fake_run(command, cwd=None, env=None, timeout=None):
        return {
            'command': list(command),
            'exit_code': 0,
            'stdout': next(outputs),
            'stderr': '',
        }

    monkeypatch.setattr(validation, 'run_command', fake_run)

    result = validation.validate({'score': ['score-tool']})

    assert result['checks_passed'] is True
    assert len(result['score_runs']) == 3
    assert result['scores'] == [3.0, 6.0, 9.0]
    assert result['score'] == 6.0


def test_validate_randomizes_paired_baseline_and_candidate_scores(monkeypatch):
    seen = []

    def fake_run(command, cwd=None, env=None, timeout=None):
        seen.append(cwd)
        return {
            'command': list(command), 'exit_code': 0,
            'stdout': '10\n' if cwd == 'baseline' else '8\n', 'stderr': '',
        }

    monkeypatch.setattr(validation, 'run_command', fake_run)
    result = validation.validate({
        'score': ['score-tool'], 'baseline_cwd': 'baseline',
        'score_seed': 3,
    }, cwd='candidate')

    assert sorted(seen) == ['baseline'] * 3 + ['candidate'] * 3
    assert seen != ['candidate'] * 3 + ['baseline'] * 3
    assert result['score'] == 8
    assert result['baseline_score'] == 10


def test_run_command_reports_timeout(monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs['timeout'])

    monkeypatch.setattr(validation.subprocess, 'run', timeout)
    result = validation.run_command(['slow'], timeout=1)

    assert result['exit_code'] == -9
    assert result['timed_out'] is True


def test_validate_records_a_failed_check():
    result = validation.validate({
        'checks': [[
            sys.executable,
            '-c',
            'import sys; print("failed check"); sys.exit(2)',
        ]],
    })

    assert result['checks_passed'] is False
    assert result['checks'][0]['exit_code'] == 2
    assert result['checks'][0]['stdout'] == 'failed check\n'


@pytest.mark.parametrize(
    'score_script, expected_error',
    [
        ('import sys; sys.exit(4)', 'command failed'),
        ('print("not numeric")', "could not convert string to float"),
    ],
)
def test_validate_reports_score_failures(score_script, expected_error):
    result = validation.validate({
        'score': [sys.executable, '-c', score_script],
    })

    assert expected_error in result['score_error']
    assert len(result['score_runs']) == 1
    assert 'score' not in result


def test_eligibility_applies_min_and_max_improvement_thresholds():
    minimum_result = {'checks_passed': True, 'score': 8.0}
    maximum_result = {'checks_passed': True, 'score': 12.5}

    assert validation.eligible(minimum_result, 10.0, 'min', 2.0)
    assert not validation.eligible(minimum_result, 10.0, 'min', 2.01)
    assert validation.eligible(maximum_result, 10.0, 'max', 2.5)
    assert not validation.eligible(maximum_result, 10.0, 'max', 2.51)
    assert validation.eligible({'checks_passed': True})
    assert not validation.eligible({'checks_passed': False})
    assert not validation.eligible({
        'checks_passed': True,
        'score_error': 'command failed',
    })


@pytest.mark.parametrize('check_exit, expected_exit', [(0, 0), (3, 1)])
def test_validation_module_cli_emits_marker_and_exit_status(
    tmp_path, check_exit, expected_exit,
):
    spec_path = tmp_path / 'validation.json'
    spec_path.write_text(json.dumps({
        'checks': [[
            sys.executable,
            '-c',
            'import sys; print("checked"); sys.exit({})'.format(check_exit),
        ]],
    }), encoding='utf-8')

    completed = subprocess.run(
        [
            sys.executable,
            '-m',
            'taskq.explore.validation',
            '--spec',
            str(spec_path),
        ],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == expected_exit
    assert completed.stdout.startswith(validation.MARKER)
    payload = json.loads(completed.stdout[len(validation.MARKER):])
    assert payload['checks_passed'] is (check_exit == 0)

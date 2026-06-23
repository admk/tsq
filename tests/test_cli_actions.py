import argparse
import io
import json
import sys

import pytest

from taskq import TOOL_NAME
from taskq.actions.add import AddAction
from taskq.actions.filter import FilterActionBase
from taskq.cli import CLI


def run_cli(args, rc_file, capsys):
    code = CLI().main(['-rc', str(rc_file), *args])
    out = capsys.readouterr().out
    return code, out


@pytest.fixture(autouse=True)
def force_tty_stdin(monkeypatch):
    monkeypatch.setattr('taskq.actions.add.STDIN_TTY', True)


def test_cli_help_lists_fake_backend(fake_backend, rc_file, capsys):
    with pytest.raises(SystemExit) as exc:
        CLI().main(['-h'])
    assert exc.value.code == 0
    assert 'fake' in capsys.readouterr().out


def test_add_single_command_and_resources(fake_backend, rc_file, capsys):
    _, out = run_cli(['add', '-G', '2', '-N', '3', 'echo', 'hi'], rc_file, capsys)
    backend = fake_backend.instances[-1]
    assert backend.calls[-1] == ('add', 'echo hi', 2, 3)
    assert 'Added: 100' in out


def test_add_preserves_shell_argument_quoting(fake_backend, rc_file, capsys):
    run_cli(['add', 'sh', '-c', 'exit 1'], rc_file, capsys)
    assert (
        'add', "sh -c 'exit 1'", None, None
    ) in fake_backend.instances[-1].calls


def test_add_depends_on_ids(fake_backend, rc_file, capsys):
    _, out = run_cli(['add', '-D', '1-2,4', 'echo', 'hi'], rc_file, capsys)
    assert (
        'add', 'echo hi', None, None, [1, 2, 4]
    ) in fake_backend.instances[-1].calls
    assert 'Added: 100' in out


def test_add_unique_skips_existing_command(fake_backend, rc_file, capsys):
    _, out = run_cli(['add', '-u', 'python', 'train.py'], rc_file, capsys)
    assert not [
        call for call in fake_backend.instances[-1].calls
        if call[0] == 'add'
    ]
    assert 'Skipped commands' in out


def test_add_from_file_and_expansion(fake_backend, rc_file, tmp_path, capsys):
    commands = tmp_path / 'commands.txt'
    commands.write_text('echo {a,b} [1-2]\n', encoding='utf-8')
    run_cli(['add', '-f', str(commands)], rc_file, capsys)
    calls = [
        call for call in fake_backend.instances[-1].calls
        if call[0] == 'add'
    ]
    assert [call[1] for call in calls] == [
        'echo a 1', 'echo b 1', 'echo a 2', 'echo b 2'
    ]


def test_add_interactive_single_attaches(fake_backend, rc_file, capsys):
    run_cli(['add', '-i', 'echo', 'hi'], rc_file, capsys)
    assert ('output', 100, 0, True) in fake_backend.instances[-1].calls


def test_add_interactive_multiple_rejected(fake_backend, rc_file, tmp_path, capsys):
    commands = tmp_path / 'commands.txt'
    commands.write_text('echo one\necho two\n', encoding='utf-8')
    _, out = run_cli(['add', '-i', '-f', str(commands)], rc_file, capsys)
    assert 'Cannot interact with multiple added jobs.' in out


def test_add_dry_run(fake_backend, rc_file, capsys):
    _, out = run_cli(['add', '-d', 'echo', 'hi'], rc_file, capsys)
    assert out.strip() == f'{TOOL_NAME} add -N 1 echo hi'
    assert not [
        call for call in fake_backend.instances[-1].calls
        if call[0] == 'add'
    ]

    _, out = run_cli(['add', '-d', '-G', '2', '-N', '3', 'echo', '@u'], rc_file, capsys)
    assert out.strip().startswith(f'{TOOL_NAME} add -G 2 -N 3 echo ')
    assert '@u' not in out

    _, out = run_cli(['add', '-d', '-D', '1-3,5', 'echo', 'hi'], rc_file, capsys)
    assert out.strip() == f'{TOOL_NAME} add -N 1 -D 1,2,3,5 echo hi'

    _, out = run_cli(['add', '-d', 'echo', 'a\nb'], rc_file, capsys)
    assert out == f"{TOOL_NAME} add -N 1 echo 'a\\nb'\n"


def test_add_invalid_dependency_id_prints_cli_error(fake_backend, rc_file, capsys):
    code = CLI().main(['-rc', str(rc_file), 'add', '-D', 'x', 'echo', 'hi'])
    captured = capsys.readouterr()

    assert code == 2
    assert f"{TOOL_NAME}: error: invalid job ID 'x'" in captured.err
    assert 'Traceback' not in captured.err


def test_add_extrapolates_stdin_arguments(monkeypatch):
    monkeypatch.setattr('taskq.actions.add.STDIN_TTY', False)
    monkeypatch.setattr(sys, 'stdin', io.StringIO('one,two\n'))
    commands = AddAction._extrapolate_inputs(['echo', '@1', '@2'], None, ',')
    assert commands == ['echo one two']


def test_add_ignores_empty_non_tty_stdin(monkeypatch):
    monkeypatch.setattr('taskq.actions.add.STDIN_TTY', False)
    monkeypatch.setattr(sys, 'stdin', io.StringIO(''))
    commands = AddAction._extrapolate_inputs(['echo', 'hi'], None, ',')
    assert commands == ['echo hi']


def test_filter_parse_ids_and_statuses():
    action = FilterActionBase('x', {'name': 'x'})
    args = argparse.Namespace(
        id='1-3,5', all=False, running=True, queued=False,
        success=False, failed=False, killed=False, interrupted=True,
    )
    action.transform_args(args)
    assert action.ids == [1, 2, 3, 5]
    assert action.filters.running
    assert action.filters.interrupted
    assert not action.filters.all


def test_invalid_default_id_prints_cli_error(fake_backend, rc_file, capsys):
    code = CLI().main(['-rc', str(rc_file), 'hello'])
    captured = capsys.readouterr()

    assert code == 2
    assert f"{TOOL_NAME}: error: invalid job ID 'hello'" in captured.err
    assert 'Traceback' not in captured.err


def test_list_info_ids_commands_outputs_export(fake_backend, rc_file, capsys):
    _, out = run_cli(['list', '-c', 'id,status,slots,gpus,gpu_ids,command,output'], rc_file, capsys)
    assert 'python train.py' in out

    _, out = run_cli(['ids'], rc_file, capsys)
    assert out.strip() == '1, 2, 3, 4'

    _, out = run_cli(['info', '1'], rc_file, capsys)
    assert 'Job 1:' in out
    assert '  status: running\n  exit_code: None' in out

    _, out = run_cli(['info', '3'], rc_file, capsys)
    assert '  status: success\n  exit_code: 0' in out

    _, out = run_cli(['info', '-i'], rc_file, capsys)
    assert 'Job 4:' in out
    assert '  status: interrupted\n  exit_code: None' in out
    assert 'Job 1:' not in out

    _, out = run_cli(['commands', '-j'], rc_file, capsys)
    assert 'python train.py' in out

    _, out = run_cli(['c', '-j'], rc_file, capsys)
    assert 'python train.py' in out

    _, out = run_cli(['outputs', '-R', '1'], rc_file, capsys)
    assert 'output-1' in out

    _, out = run_cli(['o', '-R', '1'], rc_file, capsys)
    assert 'output-1' in out

    _, out = run_cli(['export', '-e', 'json', '-t', '1', '1'], rc_file, capsys)
    payload = json.loads(out)
    assert payload[0]['output'] == 'output-1'

    _, out = run_cli(['export', '-e', 'yaml', '1'], rc_file, capsys)
    assert 'python train.py' in out

    _, out = run_cli(['export', '-e', 'toml', '1'], rc_file, capsys)
    assert 'python train.py' in out


def test_commands_escapes_control_characters(fake_backend, rc_file, capsys):
    fake_backend.jobs[0]['command'] = "echo 'a\nb'"

    _, out = run_cli(['commands', '1'], rc_file, capsys)

    assert out == "1: echo 'a\\nb'\n"


def test_outputs_follow_requires_single_job(
    fake_backend, rc_file, tmp_path, capsys
):
    result, out = run_cli(['outputs', '-F'], rc_file, capsys)
    assert result == 1
    assert 'Cannot follow multiple outputs.' in out

    output_file = tmp_path / 'out-1.log'
    output_file.write_text('line1\nline2\n', encoding='utf-8')
    fake_backend.jobs[0]['status'] = 'success'
    fake_backend.jobs[0]['output_file'] = str(output_file)

    _, out = run_cli(['outputs', '-F', '1'], rc_file, capsys)
    assert out == 'line1\nline2\n'
    assert ('output', 1, 0, True) not in fake_backend.instances[-1].calls


def test_outputs_interrupted_filter_uses_lowercase_i(fake_backend, rc_file, capsys):
    _, out = run_cli(['outputs', '-i'], rc_file, capsys)
    assert 'Job 4:' in out
    assert 'output-4' in out
    assert 'Job 1:' not in out


def test_interact_action_requires_single_job(fake_backend, rc_file, capsys):
    result, out = run_cli(['interact'], rc_file, capsys)
    assert result == 1
    assert 'Cannot interact with multiple jobs.' in out

    run_cli(['interact', '1'], rc_file, capsys)
    assert ('interact', 1) in fake_backend.instances[-1].calls


def test_wait_exits_when_jobs_finish(fake_backend, rc_file, monkeypatch, capsys):
    calls = {'count': 0}

    def fake_job_info(self, ids=None, filters=None):
        calls['count'] += 1
        if calls['count'] == 1:
            return [{'id': 1, 'status': 'running'}]
        return []

    monkeypatch.setattr(fake_backend, 'job_info', fake_job_info)
    monkeypatch.setattr('taskq.actions.read.time.sleep', lambda _: None)
    run_cli(['wait'], rc_file, capsys)
    assert calls['count'] >= 2


def test_write_actions_and_danger_guard(fake_backend, rc_file, capsys):
    with pytest.raises(SystemExit):
        run_cli(['kill'], rc_file, capsys)
    assert 'dangerous action' in capsys.readouterr().out

    run_cli(['kill', '1'], rc_file, capsys)
    assert ('kill', 1, True) in fake_backend.instances[-1].calls

    run_cli(['remove', '2-3'], rc_file, capsys)
    assert ('remove', 2, True) in fake_backend.instances[-1].calls
    assert ('remove', 3, True) in fake_backend.instances[-1].calls

    run_cli(['rerun', '1'], rc_file, capsys)
    assert ('add', 'python train.py', 1, 2) in fake_backend.instances[-1].calls

    run_cli(['requeue', '1'], rc_file, capsys)
    calls = fake_backend.instances[-1].calls
    assert ('add', 'python train.py', 1, 2) in calls
    assert ('remove', 1, True) in calls

    _, out = run_cli(['rerun', '-d', '1'], rc_file, capsys)
    assert 'python train.py' in out
    assert '<id>' in out
    calls = fake_backend.instances[-1].calls
    assert not [call for call in calls if call[0] == 'add']


def test_backend_and_config_actions(fake_backend, rc_file, capsys):
    _, out = run_cli(['backend', 'info'], rc_file, capsys)
    assert 'name = "fake"' in out

    _, out = run_cli(['backend', 'command', 'x', 'y'], rc_file, capsys)
    assert 'backend:x y' in out

    result, out = run_cli(['backend', 'command'], rc_file, capsys)
    assert result == 1
    assert 'No command provided.' in out

    _, out = run_cli(['backend', 'reset'], rc_file, capsys)
    assert 'Killed fake backend.' in out

    _, out = run_cli(['config', 'slots'], rc_file, capsys)
    assert out.strip() == '2'

    run_cli(['config', 'new.key', 'value'], rc_file, capsys)
    assert 'new' in rc_file.read_text(encoding='utf-8')

    run_cli(['config', 'new.key', 'null'], rc_file, capsys)
    assert 'value' not in rc_file.read_text(encoding='utf-8')

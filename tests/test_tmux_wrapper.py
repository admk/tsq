import json
import subprocess

from taskq.backends.tmux.backend import render_wrapper


def test_render_wrapper_uses_shell_template(tmp_path):
    output_file = tmp_path / 'job dir' / 'output.log'
    start_file = tmp_path / 'job dir' / 'start'

    wrapper = render_wrapper(
        job_id=7,
        argv=['echo', 'hi there'],
        session='taskq-test-7',
        output_file=output_file,
        start_file=start_file,
        env_exports='export FOO=bar',
        submission_id='submission-7',
    )

    assert wrapper.startswith('#!/bin/sh\n')
    assert "set -- echo 'hi there'" in wrapper
    assert 'export FOO=bar' in wrapper
    assert 'complete_job.py' not in wrapper
    assert "${TASKQ_GPU_IDS+x}" in wrapper
    assert "<<'PY'" not in wrapper
    assert 'command-result.json' in wrapper


def test_merge_wrapper_hands_off_success_without_terminal_marker(tmp_path):
    output_file = tmp_path / 'job' / 'output.log'
    start_file = tmp_path / 'job' / 'start'
    result_file = tmp_path / 'job' / 'command-result.json'
    start_file.parent.mkdir()
    start_file.touch()
    wrapper = start_file.parent / 'run.sh'
    wrapper.write_text(render_wrapper(
        job_id=7,
        argv=['sh', '-c', 'exit 0'],
        session='taskq-test-7',
        output_file=output_file,
        start_file=start_file,
        env_exports='',
        command_result_file=result_file,
        merge=True,
        submission_id='submission-7',
    ), encoding='utf-8')
    wrapper.chmod(0o700)

    result = subprocess.run([str(wrapper)], capture_output=True, text=True)

    assert result.returncode == 0
    command_result = json.loads(result_file.read_text(encoding='utf-8'))
    assert command_result['exitcode'] == 0
    assert command_result['end_time']
    assert command_result['submission_id'] == 'submission-7'
    output = output_file.read_text(encoding='utf-8')
    assert 'command completed; waiting for merge' in output
    assert 'finished with exit code' not in output


def test_merge_wrapper_keeps_terminal_marker_for_command_failure(tmp_path):
    output_file = tmp_path / 'job' / 'output.log'
    start_file = tmp_path / 'job' / 'start'
    result_file = tmp_path / 'job' / 'command-result.json'
    start_file.parent.mkdir()
    start_file.touch()
    wrapper = start_file.parent / 'run.sh'
    wrapper.write_text(render_wrapper(
        job_id=8,
        argv=['sh', '-c', 'exit 3'],
        session='taskq-test-8',
        output_file=output_file,
        start_file=start_file,
        env_exports='',
        command_result_file=result_file,
        merge=True,
        submission_id='submission-8',
    ), encoding='utf-8')
    wrapper.chmod(0o700)

    result = subprocess.run([str(wrapper)], capture_output=True, text=True)

    assert result.returncode == 3
    command_result = json.loads(result_file.read_text())
    assert command_result['exitcode'] == 3
    assert command_result['submission_id'] == 'submission-8'
    assert 'finished with exit code 3' in output_file.read_text(encoding='utf-8')

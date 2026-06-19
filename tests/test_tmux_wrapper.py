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
    )

    assert wrapper.startswith('#!/bin/sh\n')
    assert "set -- echo 'hi there'" in wrapper
    assert 'export FOO=bar' in wrapper
    assert 'complete_job.py' not in wrapper
    assert "${TASKQ_GPU_IDS+x}" in wrapper
    assert "<<'PY'" not in wrapper

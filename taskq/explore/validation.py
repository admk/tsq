import argparse
import json
import math
import os
import random
import shlex
import subprocess


MARKER = 'TASKQ_VALIDATION:'


def command_argv(command):
    return shlex.split(command) if isinstance(command, str) else list(command)


def run_command(command, cwd=None, env=None, timeout=None):
    try:
        result = subprocess.run(
            command_argv(command), cwd=cwd, env=env, timeout=timeout,
            capture_output=True, text=True,
        )
    except subprocess.TimeoutExpired as error:
        return {
            'command': command_argv(command), 'exit_code': -9,
            'stdout': (error.stdout or '')[-20000:],
            'stderr': (error.stderr or '')[-20000:], 'timed_out': True,
        }
    return {
        'command': command_argv(command),
        'exit_code': result.returncode,
        'stdout': result.stdout[-20000:],
        'stderr': result.stderr[-20000:],
    }


def parse_score(output):
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        raise ValueError('score command produced no output')
    value = float(lines[-1])
    if not math.isfinite(value):
        raise ValueError('score must be finite')
    return value


def validate(spec, cwd=None, env=None):
    timeout = spec.get('command_timeout', 1800)
    checks = [
        run_command(command, cwd, env, timeout)
        for command in spec.get('checks', [])]
    result = {
        'checks': checks,
        'checks_passed': all(item['exit_code'] == 0 for item in checks),
    }
    score_command = spec.get('score')
    if not score_command:
        return result
    repeats = int(spec.get('score_repeats', 3))
    baseline_cwd = spec.get('baseline_cwd')
    order = ['candidate'] * repeats
    if baseline_cwd:
        order += ['baseline'] * repeats
        random.Random(spec.get('score_seed')).shuffle(order)
    scores = {'candidate': [], 'baseline': []}
    score_runs = []
    for label in order:
        run_cwd = baseline_cwd if label == 'baseline' else cwd
        run = run_command(score_command, run_cwd, env, timeout)
        run['sample'] = label
        score_runs.append(run)
        if run['exit_code'] != 0:
            result.update({'score_runs': score_runs, 'score_error': 'command failed'})
            return result
        try:
            scores[label].append(parse_score(run['stdout']))
        except ValueError as error:
            result.update({'score_runs': score_runs, 'score_error': str(error)})
            return result
    for values in scores.values():
        values.sort()
    result.update({
        'score_runs': score_runs,
        'score_order': order,
        'scores': scores['candidate'],
        'score': scores['candidate'][len(scores['candidate']) // 2],
    })
    if baseline_cwd:
        result.update({
            'baseline_scores': scores['baseline'],
            'baseline_score': scores['baseline'][len(scores['baseline']) // 2],
        })
    return result


def eligible(result, baseline=None, direction=None, minimum=0):
    if not result.get('checks_passed', False) or result.get('score_error'):
        return False
    if baseline is None or result.get('score') is None:
        return True
    delta = result['score'] - baseline
    improvement = -delta if direction == 'min' else delta
    return improvement >= float(minimum)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--spec', required=True)
    args = parser.parse_args(argv)
    with open(args.spec, 'r', encoding='utf-8') as stream:
        spec = json.load(stream)
    result = validate(spec, cwd=os.getcwd(), env=os.environ.copy())
    print(MARKER + json.dumps(result, separators=(',', ':'), sort_keys=True))
    return 0 if result['checks_passed'] and not result.get('score_error') else 1


if __name__ == '__main__':
    raise SystemExit(main())

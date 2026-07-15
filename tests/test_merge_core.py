import fcntl
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from taskq.backends.base import BackendError
from taskq.backends.tmux import lifecycle
from taskq.merge import build_merge_spec, cancel_merge_job, register_merge_job
from taskq.merge.controller import MergeController
from taskq.merge.state import MergeState


def git(repo, *args, check=True, env=None):
    result = subprocess.run(
        ['git', '-C', str(repo), *map(str, args)],
        capture_output=True,
        check=False,
        text=True,
        env=env,
    )
    if check and result.returncode:
        raise AssertionError(
            'git {} failed:\n{}'.format(' '.join(map(str, args)), result.stderr))
    return result.stdout.strip() if check else result


def init_repo(path):
    path.mkdir(parents=True)
    git(path, 'init', '-b', 'main')
    git(path, 'config', 'user.name', 'Taskq Tests')
    git(path, 'config', 'user.email', 'taskq@example.invalid')
    git(path, 'config', 'commit.gpgsign', 'false')
    (path / 'tracked.txt').write_text('base\n', encoding='utf-8')
    git(path, 'add', 'tracked.txt')
    git(path, 'commit', '-m', 'base')
    return git(path, 'rev-parse', 'HEAD')


class FakeBackend:
    """Only the resolver-facing backend surface needed by MergeController."""

    name = 'fake'

    def __init__(self, state_dir):
        self.state_dir = Path(state_dir)
        self.config = {'queue': 'test', 'broker_interval': 0.1}
        self.jobs = {}
        self.add_calls = []
        self.controller_calls = []
        self.killed = []
        self.add_error = None
        self._next_id = 100

    def register_controller(self, name, command, **kwargs):
        self.controller_calls.append((name, list(command), kwargs))

    def add(self, command, gpus, slots, **kwargs):
        if self.add_error is not None:
            raise self.add_error
        job_id = self._next_id
        self._next_id += 1
        self.jobs[job_id] = {
            'id': job_id,
            'status': 'queued',
            'exitcode': None,
            'metadata': kwargs.get('metadata', {}),
        }
        self.add_calls.append({
            'id': job_id,
            'command': command,
            'gpus': gpus,
            'slots': slots,
            **kwargs,
        })
        return job_id

    def full_info(self, ids=None):
        values = list(self.jobs.values())
        if ids is not None:
            values = [value for value in values if value['id'] in ids]
        return [dict(value) for value in values]

    def finish(self, job_id, status='success', output=''):
        self.jobs[int(job_id)].update({
            'status': status,
            'exitcode': 0 if status == 'success' else 1,
            'output': output,
        })

    def output(self, info, tail):
        return self.jobs.get(int(info['id']), {}).get('output', '')

    def kill(self, info):
        job_id = int(info['id'])
        self.killed.append(job_id)
        if job_id in self.jobs:
            self.jobs[job_id]['status'] = 'killed'


class FakeBackendFactory:
    def __init__(self, backend):
        self.backend = backend
        self.calls = []

    def __call__(self, spec):
        self.calls.append(spec)
        return self.backend


@dataclass
class MergeHarness:
    repo: Path
    source: Path
    base_head: str
    state: MergeState
    controller: MergeController
    backend: FakeBackend
    backend_factory: FakeBackendFactory
    meta: dict
    request_id: int
    lane_id: str

    @property
    def request(self):
        return self.state.get_request(self.request_id)

    @property
    def lane(self):
        return self.state.get_lane(self.lane_id)


def register_additional_merge_job(harness, job_id, changes, env=None):
    """Register another ready parent against the harness's original base."""
    source = harness.repo.parent / 'job-worktree-{}'.format(job_id)
    git(harness.repo, 'worktree', 'add', '--detach', source, harness.base_head)
    for relative, text in changes.items():
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding='utf-8')

    job_dir = harness.backend.state_dir / 'jobs' / str(job_id)
    job_dir.mkdir(parents=True)
    submission_id = 'submission-{}'.format(job_id)
    command_result = job_dir / 'command-result.json'
    command_result.write_text(
        json.dumps({
            'exitcode': 0,
            'submission_id': submission_id,
        }) + '\n', encoding='utf-8')
    env_file = job_dir / 'env.json'
    env_file.write_text(json.dumps(env or {}) + '\n', encoding='utf-8')
    meta_file = job_dir / 'meta.json'
    meta = {
        'id': job_id,
        'submission_id': submission_id,
        'command': 'edit {}'.format(', '.join(sorted(changes))),
        'git_root': str(harness.repo),
        'git_worktree': str(source),
        'git_commit': harness.base_head,
        'wrapper': str(job_dir / 'wrapper.sh'),
        'env_file': str(env_file),
        'command_result_file': str(command_result),
        'merge_status_file': str(job_dir / 'merge-status.json'),
        'merge_result_file': str(job_dir / 'merge-result.json'),
        'merge': harness.meta['merge'],
    }
    meta_file.write_text(json.dumps(meta) + '\n', encoding='utf-8')
    request = register_merge_job(harness.backend, meta)
    return request, source, meta


def set_ready_order(state, request_ids):
    """Give command sidecars deterministic ready-time ordering."""
    for index, request_id in enumerate(request_ids, 1):
        path = Path(state.get_request(request_id)['command_result_file'])
        timestamp = 1_000_000_000 + index
        os.utime(path, ns=(timestamp, timestamp))


@pytest.fixture
def merge_harness(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'cache'))
    states = []
    serial = 0

    def create(change_text='from job\n'):
        nonlocal serial
        serial += 1
        root = tmp_path / 'case-{}'.format(serial)
        repo = root / 'repo'
        base_head = init_repo(repo)
        source = root / 'job-worktree'
        git(repo, 'worktree', 'add', '--detach', source, base_head)
        if change_text is not None:
            (source / 'job.txt').write_text(change_text, encoding='utf-8')

        backend = FakeBackend(root / 'backend-state')
        job_dir = backend.state_dir / 'jobs' / '1'
        job_dir.mkdir(parents=True)
        submission_id = 'submission-{}-1'.format(serial)
        command_result = job_dir / 'command-result.json'
        status_file = job_dir / 'merge-status.json'
        result_file = job_dir / 'merge-result.json'
        command_result.write_text(
            json.dumps({
                'exitcode': 0,
                'submission_id': submission_id,
            }) + '\n', encoding='utf-8')

        spec = build_merge_spec({
            'merge': {
                'command': ['fake-resolver', '{}'],
                'conflict_prompt': 'resolve $change_head into $target_branch',
                'timeout': 30,
            },
        }, repo)
        meta = {
            'id': 1,
            'submission_id': submission_id,
            'command': 'write job.txt',
            'git_root': str(repo),
            'git_worktree': str(source),
            'git_commit': base_head,
            'wrapper': str(job_dir / 'wrapper.sh'),
            'command_result_file': str(command_result),
            'merge_status_file': str(status_file),
            'merge_result_file': str(result_file),
            'merge': spec,
        }
        (job_dir / 'meta.json').write_text(
            json.dumps(meta) + '\n', encoding='utf-8')
        request = register_merge_job(backend, meta)
        state = MergeState(spec['state_path'])
        states.append(state)
        backend_factory = FakeBackendFactory(backend)
        return MergeHarness(
            repo=repo,
            source=source,
            base_head=base_head,
            state=state,
            controller=MergeController(state, backend_factory=backend_factory),
            backend=backend,
            backend_factory=backend_factory,
            meta=meta,
            request_id=request['id'],
            lane_id=request['lane_id'],
        )

    yield create

    for state in states:
        state.close()


def test_build_merge_spec_captures_branch_and_requires_explicit_detached_target(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv('XDG_CACHE_HOME', str(tmp_path / 'cache'))
    repo = tmp_path / 'repo'
    head = init_repo(repo)
    git(repo, 'switch', '-c', 'topic')

    spec = build_merge_spec({}, repo)

    assert spec['source_head'] == head
    assert spec['target_branch'] == 'topic'
    assert spec['target_ref'] == 'refs/heads/topic'
    assert spec['target_head'] == head

    git(repo, 'switch', '--detach', head)
    with pytest.raises(BackendError, match='detached HEAD requires an explicit'):
        build_merge_spec({}, repo)

    detached = build_merge_spec({}, repo, branch='main')
    assert detached['source_head'] == head
    assert detached['target_branch'] == 'main'
    assert detached['target_ref'] == 'refs/heads/main'


def test_merge_state_assigns_fifo_when_requests_become_ready_and_persists(tmp_path):
    path = tmp_path / 'state.sqlite3'
    lane = {
        'id': 'lane-1',
        'repo_root': str(tmp_path / 'repo'),
        'common_dir': str(tmp_path / 'repo' / '.git'),
        'target_branch': 'main',
        'target_ref': 'refs/heads/main',
        'staging_ref': 'refs/taskq/merge/lanes/lane-1/staging',
        'staging_worktree': str(tmp_path / 'staging'),
    }

    def value(number):
        job_dir = tmp_path / 'jobs' / str(number)
        return {
            'lane_id': lane['id'],
            'job_key': 'queue:{}'.format(number),
            'parent_job_id': number,
            'job_dir': str(job_dir),
            'meta_file': str(job_dir / 'meta.json'),
            'command_result_file': str(job_dir / 'command-result.json'),
            'status_file': str(job_dir / 'merge-status.json'),
            'result_file': str(job_dir / 'merge-result.json'),
            'source_worktree': str(tmp_path / 'sources' / str(number)),
            'source_base': 'base',
            'spec': {'resolver': {'command': ['fake', '{}'], 'timeout': 1}},
            'backend': {'name': 'fake', 'state_dir': str(tmp_path / 'backend')},
        }

    with MergeState(path) as state:
        state.ensure_lane(lane)
        first = state.add_request(value(1))
        second = state.add_request(value(2))

        ready_second = state.mark_ready(second['id'], 'ref-2', 'head-2', 'tree-2')
        ready_first = state.mark_ready(first['id'], 'ref-1', 'head-1', 'tree-1')
        duplicate = state.mark_ready(second['id'], 'other-ref', 'other-head', 'other')

        assert (ready_second['sequence'], ready_first['sequence']) == (1, 2)
        assert duplicate['sequence'] == 1
        assert duplicate['change_head'] == 'head-2'
        assert [row['parent_job_id'] for row in state.list_requests()] == [2, 1]

    with MergeState(path) as state:
        third = state.add_request(value(3))
        noop = state.mark_ready(third['id'], None, None, 'tree-3')

        assert noop['sequence'] == 3
        assert noop['status'] == 'landed'
        assert noop['result'] == {'noop': True}


def test_controller_lands_clean_change_and_late_cancel_preserves_success(
    merge_harness,
):
    harness = merge_harness()

    assert harness.controller.reconcile()

    request = harness.request
    landed_head = git(harness.repo, 'rev-parse', 'refs/heads/main')
    result = json.loads(Path(request['result_file']).read_text(encoding='utf-8'))
    assert request['status'] == 'landed'
    assert landed_head != harness.base_head
    assert git(harness.repo, 'rev-parse', 'HEAD') == landed_head
    assert (harness.repo / 'job.txt').read_text(encoding='utf-8') == 'from job\n'
    assert result['status'] == 'success'
    assert result['merge']['stage'] == 'landed'
    assert harness.backend.add_calls == []

    # If landing wins the controller lock, a concurrent kill/cancel must not
    # rewrite the already-observable success into cancellation.
    after_cancel = cancel_merge_job(harness.meta)
    assert after_cancel['status'] == 'landed'
    assert harness.request['status'] == 'landed'
    assert json.loads(Path(request['result_file']).read_text(
        encoding='utf-8'))['status'] == 'success'


def test_controller_cleanly_cherry_picks_tracked_edit_with_signing_enabled(
    merge_harness,
):
    harness = merge_harness(change_text=None)
    (harness.source / 'tracked.txt').write_text(
        'edited by job\n', encoding='utf-8')
    # Taskq-owned integration commits must not inherit interactive/global
    # signing requirements from the destination repository.
    git(harness.repo, 'config', 'commit.gpgsign', 'true')

    harness.controller.reconcile()

    assert harness.request['status'] == 'landed'
    assert (harness.repo / 'tracked.txt').read_text(
        encoding='utf-8') == 'edited by job\n'
    assert harness.backend.add_calls == []


def test_controller_completes_noop_without_moving_target(merge_harness):
    harness = merge_harness(change_text=None)

    harness.controller.reconcile()

    request = harness.request
    result = json.loads(Path(request['result_file']).read_text(encoding='utf-8'))
    assert request['status'] == 'landed'
    assert request['result'] == {'noop': True}
    assert git(harness.repo, 'rev-parse', 'refs/heads/main') == harness.base_head
    assert result['status'] == 'success'
    assert result['merge']['noop'] is True
    assert not Path(harness.lane['staging_worktree']).exists()
    assert harness.backend.add_calls == []


def test_dirty_checked_out_target_stages_without_ref_movement_then_lands(
    merge_harness,
):
    harness = merge_harness()
    dirty = harness.repo / 'keep-me.txt'
    dirty.write_bytes(b'user work must survive\n')
    before_status = git(harness.repo, 'status', '--porcelain')

    harness.controller.reconcile()

    request = harness.request
    lane = harness.lane
    assert request['status'] == 'staged'
    assert git(harness.repo, 'rev-parse', 'refs/heads/main') == harness.base_head
    assert git(harness.repo, 'rev-parse', 'HEAD') == harness.base_head
    assert git(harness.repo, 'status', '--porcelain') == before_status
    assert dirty.read_bytes() == b'user work must survive\n'
    assert git(harness.repo, 'rev-parse', lane['staging_ref']) == request['staged_head']
    assert request['staged_head'] != harness.base_head
    assert 'dirty' in lane['blocked_reason']
    assert Path(lane['staging_worktree']).exists()

    dirty.unlink()
    harness.controller.reconcile()

    request = harness.request
    assert request['status'] == 'landed'
    assert git(harness.repo, 'rev-parse', 'refs/heads/main') == request['staged_head']
    assert git(harness.repo, 'rev-parse', 'HEAD') == request['staged_head']
    assert (harness.repo / 'job.txt').read_text(encoding='utf-8') == 'from job\n'
    assert harness.lane['blocked_reason'] is None


def test_unchecked_target_branch_advances_without_moving_current_checkout(
    merge_harness,
):
    harness = merge_harness()
    git(harness.repo, 'switch', '-c', 'observer')
    observer_head = git(harness.repo, 'rev-parse', 'HEAD')

    harness.controller.reconcile()

    request = harness.request
    assert request['status'] == 'landed'
    assert git(harness.repo, 'rev-parse', 'refs/heads/main') == request['staged_head']
    assert git(harness.repo, 'symbolic-ref', '--short', 'HEAD') == 'observer'
    assert git(harness.repo, 'rev-parse', 'HEAD') == observer_head
    assert git(harness.repo, 'status', '--porcelain') == ''
    assert not (harness.repo / 'job.txt').exists()


def test_target_advancement_rebuilds_staging_before_landing(merge_harness):
    harness = merge_harness()
    blocker = harness.repo / 'local.txt'
    blocker.write_text('temporarily dirty\n', encoding='utf-8')
    harness.controller.reconcile()
    first_staged_head = harness.request['staged_head']
    assert harness.request['status'] == 'staged'

    blocker.unlink()
    (harness.repo / 'upstream.txt').write_text('upstream\n', encoding='utf-8')
    git(harness.repo, 'add', 'upstream.txt')
    git(harness.repo, 'commit', '-m', 'advance destination')
    advanced_head = git(harness.repo, 'rev-parse', 'HEAD')

    harness.controller.reconcile()

    assert harness.request['status'] == 'landed'
    assert harness.request['staged_head'] != first_staged_head
    assert git(harness.repo, 'merge-base', '--is-ancestor', advanced_head, 'HEAD') == ''
    assert (harness.repo / 'upstream.txt').read_text(
        encoding='utf-8') == 'upstream\n'
    assert (harness.repo / 'job.txt').read_text(encoding='utf-8') == 'from job\n'


def test_reconcile_recovers_target_fast_forward_before_database_finalize(
    merge_harness,
):
    harness = merge_harness()
    blocker = harness.repo / 'local.txt'
    blocker.write_text('temporarily dirty\n', encoding='utf-8')
    harness.controller.reconcile()
    staged = harness.request
    assert staged['status'] == 'staged'

    blocker.unlink()
    git(harness.repo, 'merge', '--ff-only', staged['staged_head'])
    assert harness.request['status'] == 'staged'

    harness.controller.reconcile()

    result = json.loads(Path(staged['result_file']).read_text(encoding='utf-8'))
    assert harness.request['status'] == 'landed'
    assert result['status'] == 'success'
    assert result['merge']['recovered'] is True
    assert git(harness.repo, 'rev-parse', 'HEAD') == staged['staged_head']


def test_ignored_untracked_collision_waits_without_overwrite_then_lands(
    merge_harness,
):
    harness = merge_harness(change_text=None)
    exclude = harness.repo / '.git' / 'info' / 'exclude'
    exclude.write_text(
        'incoming.txt\nunrelated.cache\n', encoding='utf-8')

    incoming = harness.source / 'incoming.txt'
    incoming.write_bytes(b'bytes from merge job\n')
    # A command may intentionally force-add or commit an otherwise ignored
    # generated path; the synthetic final-tree snapshot must preserve it.
    git(harness.source, 'add', '-f', 'incoming.txt')
    local_collision = harness.repo / 'incoming.txt'
    local_collision.write_bytes(b'ignored local bytes\n')
    unrelated = harness.repo / 'unrelated.cache'
    unrelated.write_bytes(b'unrelated ignored artifact\n')
    assert git(harness.repo, 'status', '--porcelain') == ''

    harness.controller.reconcile()

    request = harness.request
    assert request['status'] == 'staged'
    assert git(harness.repo, 'rev-parse', 'refs/heads/main') == harness.base_head
    assert git(harness.repo, 'rev-parse', 'HEAD') == harness.base_head
    assert local_collision.read_bytes() == b'ignored local bytes\n'
    assert unrelated.read_bytes() == b'unrelated ignored artifact\n'
    assert 'incoming.txt' in harness.lane['blocked_reason']
    assert 'unrelated.cache' not in harness.lane['blocked_reason']

    local_collision.unlink()
    harness.controller.reconcile()

    request = harness.request
    assert request['status'] == 'landed'
    assert git(harness.repo, 'rev-parse', 'refs/heads/main') == request['staged_head']
    assert git(harness.repo, 'rev-parse', 'HEAD') == request['staged_head']
    assert local_collision.read_bytes() == b'bytes from merge job\n'
    assert unrelated.read_bytes() == b'unrelated ignored artifact\n'


def test_casefolded_ignored_collision_waits_on_case_insensitive_worktree(
    merge_harness,
):
    harness = merge_harness(change_text=None)
    ignorecase = git(
        harness.repo, 'config', '--bool', '--get', 'core.ignorecase',
        check=False,
    )
    if ignorecase.returncode or ignorecase.stdout.strip() != 'true':
        pytest.skip('requires a case-insensitive Git worktree')

    exclude = harness.repo / '.git' / 'info' / 'exclude'
    exclude.write_text('incoming.txt\n', encoding='utf-8')
    incoming = harness.source / 'Incoming.txt'
    incoming.write_bytes(b'bytes from merge job\n')
    git(harness.source, 'add', '-f', 'Incoming.txt')
    local_collision = harness.repo / 'incoming.txt'
    local_collision.write_bytes(b'ignored local bytes\n')
    assert git(harness.repo, 'status', '--porcelain') == ''

    harness.controller.reconcile()

    request = harness.request
    assert request['status'] == 'staged'
    assert git(harness.repo, 'rev-parse', 'HEAD') == harness.base_head
    assert local_collision.read_bytes() == b'ignored local bytes\n'
    assert 'Incoming.txt' in harness.lane['blocked_reason']
    assert 'incoming.txt' in harness.lane['blocked_reason']

    local_collision.unlink()
    harness.controller.reconcile()

    request = harness.request
    assert request['status'] == 'landed'
    assert (harness.repo / 'Incoming.txt').read_bytes() == b'bytes from merge job\n'


def test_fifo_conflict_runs_one_inherited_environment_resolver_then_lands(
    merge_harness,
):
    harness = merge_harness(change_text=None)
    (harness.source / 'tracked.txt').write_text('first change\n', encoding='utf-8')
    parent_env = {
        'PARENT_ONLY': 'inherited',
        'GIT_EDITOR': 'true',
        'GIT_DIR': '/tmp/must-not-leak',
        'GIT_CONFIG_PARAMETERS': "'core.hooksPath'='/tmp/unsafe-hooks'",
        'GIT_CONFIG_COUNT': '1',
        'GIT_CONFIG_KEY_0': 'fetch.prune',
        'GIT_CONFIG_VALUE_0': 'true',
    }
    second, _, _ = register_additional_merge_job(
        harness, 2, {'tracked.txt': 'second change\n'}, env=parent_env)
    set_ready_order(harness.state, [harness.request_id, second['id']])
    git(harness.repo, 'config', 'commit.gpgsign', 'true')

    harness.controller.reconcile()

    first = harness.request
    second = harness.state.get_request(second['id'])
    assert (first['sequence'], second['sequence']) == (1, 2)
    assert first['status'] == 'staged'
    assert second['status'] == 'resolving'
    assert len(harness.backend.add_calls) == 1
    call = harness.backend.add_calls[0]
    assert call['gpus'] == 0
    assert call['slots'] == 1
    assert call['workspace_owner'] == 'merge'
    assert call['cwd'] == harness.lane['staging_worktree']
    assert call['metadata']['role'] == 'merge-resolver'
    assert call['metadata']['merge_resolver']['request_id'] == second['id']

    resolver_env = call['env']
    assert resolver_env['PARENT_ONLY'] == 'inherited'
    assert resolver_env['GIT_EDITOR'] == 'true'
    assert resolver_env['GIT_DIR'] is None
    assert resolver_env['GIT_CONFIG_PARAMETERS'] is None
    assert resolver_env['GIT_CONFIG_COUNT'] == '5'
    assert [
        (resolver_env['GIT_CONFIG_KEY_{}'.format(index)],
         resolver_env['GIT_CONFIG_VALUE_{}'.format(index)])
        for index in range(5)
    ] == [
        ('fetch.prune', 'true'),
        ('user.name', 'taskq'),
        ('user.email', 'taskq@localhost'),
        ('commit.gpgSign', 'false'),
        ('core.hooksPath', '/dev/null'),
    ]

    staging = Path(call['cwd'])
    (staging / 'tracked.txt').write_text(
        'first and second resolved\n', encoding='utf-8')
    execution_env = dict(os.environ)
    for key, value in resolver_env.items():
        if value is None:
            execution_env.pop(key, None)
        else:
            execution_env[key] = value
    git(staging, 'add', 'tracked.txt', env=execution_env)
    git(staging, 'cherry-pick', '--continue', env=execution_env)
    harness.backend.finish(call['id'])

    harness.controller.reconcile()

    first = harness.request
    second = harness.state.get_request(second['id'])
    assert first['status'] == second['status'] == 'landed'
    assert len(harness.backend.add_calls) == 1
    assert (harness.repo / 'tracked.txt').read_text(
        encoding='utf-8') == 'first and second resolved\n'
    assert git(harness.repo, 'rev-parse', 'refs/heads/main') == second['staged_head']


def test_resolved_delta_replays_after_target_moves_without_second_resolver(
    merge_harness,
):
    harness = merge_harness(change_text=None)
    (harness.source / 'tracked.txt').write_text('first change\n', encoding='utf-8')
    second, _, _ = register_additional_merge_job(
        harness, 2, {'tracked.txt': 'second change\n'})
    set_ready_order(harness.state, [harness.request_id, second['id']])
    blocker = harness.repo / 'local.tmp'
    blocker.write_text('keep destination dirty\n', encoding='utf-8')

    harness.controller.reconcile()
    call = harness.backend.add_calls[0]
    staging = Path(call['cwd'])
    (staging / 'tracked.txt').write_text(
        'first and second resolved\n', encoding='utf-8')
    git(staging, 'add', 'tracked.txt')
    git(staging, 'cherry-pick', '--continue')
    harness.backend.finish(call['id'])
    harness.controller.reconcile()

    second = harness.state.get_request(second['id'])
    resolved_head = second['result']['resolved_head']
    assert second['status'] == 'staged'
    assert second['resolver_attempts'] == 1
    assert git(
        harness.repo, 'rev-parse', second['result']['resolved_ref']) == resolved_head

    blocker.unlink()
    (harness.repo / 'upstream.txt').write_text('upstream\n', encoding='utf-8')
    git(harness.repo, 'add', 'upstream.txt')
    git(harness.repo, 'commit', '-m', 'advance target after resolution')
    advanced = git(harness.repo, 'rev-parse', 'HEAD')

    harness.controller.reconcile()

    second = harness.state.get_request(second['id'])
    assert harness.request['status'] == second['status'] == 'landed'
    assert len(harness.backend.add_calls) == 1
    assert second['resolver_attempts'] == 1
    assert second['result']['resolved_head'] == resolved_head
    assert git(harness.repo, 'merge-base', '--is-ancestor', advanced, 'HEAD') == ''
    assert (harness.repo / 'tracked.txt').read_text(
        encoding='utf-8') == 'first and second resolved\n'
    assert (harness.repo / 'upstream.txt').read_text(
        encoding='utf-8') == 'upstream\n'


def test_failed_conflict_resolver_drops_only_request_and_lands_later_fifo_work(
    merge_harness,
):
    harness = merge_harness(change_text=None)
    (harness.source / 'tracked.txt').write_text('first change\n', encoding='utf-8')
    second, second_source, second_meta = register_additional_merge_job(
        harness, 2, {'tracked.txt': 'conflicting change\n'})
    third, _, _ = register_additional_merge_job(
        harness, 3, {'later.txt': 'later work\n'})
    set_ready_order(
        harness.state, [harness.request_id, second['id'], third['id']])

    harness.controller.reconcile()

    second = harness.state.get_request(second['id'])
    third = harness.state.get_request(third['id'])
    assert harness.request['status'] == 'staged'
    assert second['status'] == 'resolving'
    assert third['status'] == 'queued'
    assert (second['sequence'], third['sequence']) == (2, 3)
    assert len(harness.backend.add_calls) == 1

    resolver_id = harness.backend.add_calls[0]['id']
    harness.backend.finish(
        resolver_id, status='failed', output='resolver could not reconcile intent')
    harness.controller.reconcile()

    first = harness.request
    second = harness.state.get_request(second['id'])
    third = harness.state.get_request(third['id'])
    failure = json.loads(Path(second['result_file']).read_text(encoding='utf-8'))
    assert first['status'] == 'landed'
    assert second['status'] == 'failed'
    assert third['status'] == 'landed'
    assert failure['status'] == 'failed'
    assert failure['failure_phase'] == 'merge'
    assert 'resolver could not reconcile intent' in failure['error']
    assert len(harness.backend.add_calls) == 1
    assert (harness.repo / 'tracked.txt').read_text(
        encoding='utf-8') == 'first change\n'
    assert (harness.repo / 'later.txt').read_text(encoding='utf-8') == 'later work\n'
    assert second_source.exists()
    assert (second_source / 'tracked.txt').read_text(
        encoding='utf-8') == 'conflicting change\n'
    assert git(
        harness.repo, 'show-ref', '--verify', second['change_ref'],
        check=False,
    ).returncode == 0

    removed = cancel_merge_job(second_meta, remove=True)

    assert removed['status'] == 'failed'
    assert git(
        harness.repo, 'show-ref', '--verify', second['change_ref'],
        check=False,
    ).returncode != 0


def test_resolver_launch_failure_fails_only_conflicted_fifo_request(
    merge_harness,
):
    harness = merge_harness(change_text=None)
    (harness.source / 'tracked.txt').write_text('first change\n', encoding='utf-8')
    second, _, _ = register_additional_merge_job(
        harness, 2, {'tracked.txt': 'conflicting change\n'})
    third, _, _ = register_additional_merge_job(
        harness, 3, {'later.txt': 'later work\n'})
    set_ready_order(
        harness.state, [harness.request_id, second['id'], third['id']])
    harness.backend.add_error = BackendError('resolver queue unavailable')

    assert harness.controller.reconcile()

    first = harness.request
    second = harness.state.get_request(second['id'])
    third = harness.state.get_request(third['id'])
    failure = json.loads(Path(second['result_file']).read_text(encoding='utf-8'))
    assert first['status'] == 'landed'
    assert second['status'] == 'failed'
    assert third['status'] == 'landed'
    assert failure['failure_phase'] == 'merge'
    assert 'could not launch merge conflict resolver' in failure['error']
    assert 'resolver queue unavailable' in failure['error']
    assert harness.backend.add_calls == []
    assert (harness.repo / 'tracked.txt').read_text(
        encoding='utf-8') == 'first change\n'
    assert (harness.repo / 'later.txt').read_text(
        encoding='utf-8') == 'later work\n'


@pytest.mark.parametrize('operation', ['--abort', '--skip'])
def test_resolver_cannot_claim_success_after_discarding_conflicted_change(
    merge_harness, operation,
):
    harness = merge_harness(change_text=None)
    (harness.source / 'tracked.txt').write_text('first change\n', encoding='utf-8')
    second, _, _ = register_additional_merge_job(
        harness, 2, {'tracked.txt': 'conflicting change\n'})
    set_ready_order(harness.state, [harness.request_id, second['id']])
    harness.controller.reconcile()
    second = harness.state.get_request(second['id'])
    assert second['status'] == 'resolving'

    git(harness.lane['staging_worktree'], 'cherry-pick', operation)
    harness.backend.finish(harness.backend.add_calls[0]['id'], status='success')
    harness.controller.reconcile()

    second = harness.state.get_request(second['id'])
    failure = json.loads(Path(second['result_file']).read_text(encoding='utf-8'))
    assert harness.request['status'] == 'landed'
    assert second['status'] == 'failed'
    assert failure['failure_phase'] == 'merge'
    assert 'completed clean cherry-pick' in failure['error']
    assert (harness.repo / 'tracked.txt').read_text(
        encoding='utf-8') == 'first change\n'


def test_terminal_database_state_reprojects_missing_result_sidecar(merge_harness):
    harness = merge_harness()
    harness.controller.reconcile()
    request = harness.request
    result_file = Path(request['result_file'])
    assert request['status'] == 'landed'
    result_file.unlink()

    harness.controller.reconcile()

    recovered = json.loads(result_file.read_text(encoding='utf-8'))
    assert harness.request['status'] == 'landed'
    assert recovered['status'] == 'success'
    assert recovered['merge']['stage'] == 'landed'
    assert recovered['merge']['recovered'] is True


def test_backend_reset_job_id_reuse_cannot_receive_old_terminal_projection(
    merge_harness,
):
    harness = merge_harness()
    harness.controller.reconcile()
    old = harness.request
    old_payload = json.loads(Path(old['result_file']).read_text(encoding='utf-8'))
    old_submission = old['spec']['submission_id']
    job_dir = Path(old['job_dir'])

    # backend_reset removes the per-job directory but deliberately leaves the
    # repository merge journal.  The next backend submission can reuse ID 1.
    shutil.rmtree(job_dir)
    job_dir.mkdir(parents=True)
    new_submission = 'submission-after-reset'
    new_merge = dict(harness.meta['merge'], submission_id=new_submission)
    new_meta = dict(harness.meta)
    new_meta.update({
        'submission_id': new_submission,
        'status': 'running',
        'exitcode': None,
        'merge': new_merge,
    })
    Path(job_dir / 'meta.json').write_text(
        json.dumps(new_meta) + '\n', encoding='utf-8')
    new_request = register_merge_job(harness.backend, new_meta)
    result_file = Path(new_request['result_file'])
    assert new_request['spec']['submission_id'] == new_submission
    assert new_request['id'] != old['id']
    assert old_submission != new_submission
    assert not result_file.exists()

    harness.controller.reconcile()

    assert harness.state.get_request(new_request['id'])['status'] == 'waiting'
    assert harness.state.active() == 1
    assert not result_file.exists()

    # Even if a stale writer publishes the prior result after reuse, the
    # backend lifecycle must reject its embedded submission UUID.
    result_file.write_text(json.dumps(old_payload) + '\n', encoding='utf-8')
    new_meta.update({
        'status': 'merging',
        'merge_result_file': str(result_file),
    })
    lifecycle.refresh_merge(new_meta, '2024-01-01T00:00:00')
    assert new_meta['status'] == 'merging'
    assert new_meta['merge']['submission_id'] == new_submission
    assert new_meta['merge']['stage'] != 'landed'


def test_waiting_request_retires_when_parent_failed_without_command_sidecar(
    merge_harness,
):
    harness = merge_harness()
    request = harness.request
    Path(request['command_result_file']).unlink()
    parent = dict(harness.meta, status='failed', exitcode=17)
    Path(request['meta_file']).write_text(
        json.dumps(parent) + '\n', encoding='utf-8')

    harness.controller.reconcile()

    request = harness.request
    assert request['status'] == 'cancelled'
    assert request['result'] == {
        'command_exitcode': 17,
        'merge_skipped': True,
        'parent_status': 'failed',
        'reason': 'atomic command-result sidecar is missing',
    }
    assert git(harness.repo, 'rev-parse', 'refs/heads/main') == harness.base_head
    assert harness.state.active() == 0


def test_lock_loser_does_not_refresh_controller_heartbeat(merge_harness):
    harness = merge_harness()
    heartbeat = harness.controller.heartbeat_path
    heartbeat.parent.mkdir(parents=True, exist_ok=True)
    heartbeat.write_text('leader heartbeat\n', encoding='utf-8')
    old_timestamp = 1_000_000_000
    os.utime(heartbeat, ns=(old_timestamp, old_timestamp))

    with open(harness.controller.lock_path, 'a', encoding='utf-8') as leader_lock:
        fcntl.flock(leader_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert harness.controller.reconcile() is False

    assert heartbeat.read_text(encoding='utf-8') == 'leader heartbeat\n'
    assert heartbeat.stat().st_mtime_ns == old_timestamp


def test_resolver_timeout_interprets_naive_tmux_time_as_local_wall_clock():
    started = (
        datetime.now().astimezone() - timedelta(seconds=10)
    ).replace(tzinfo=None).isoformat()
    request = {'spec': {'resolver': {'timeout': 1}}}

    assert MergeController._resolver_timed_out(
        request, {'start_time': started}) is True


def test_cancelled_staged_request_never_lands_and_cleans_owned_staging(
    merge_harness,
):
    harness = merge_harness()
    dirty = harness.repo / 'keep-me.txt'
    dirty.write_text('local work\n', encoding='utf-8')
    harness.controller.reconcile()
    staged = harness.request
    staging_ref = harness.lane['staging_ref']
    staging_worktree = Path(harness.lane['staging_worktree'])
    change_ref = staged['change_ref']
    assert staged['status'] == 'staged'

    cancelled = cancel_merge_job(harness.meta)

    assert cancelled['status'] == 'cancelled'
    assert not staging_worktree.exists()
    assert git(
        harness.repo, 'show-ref', '--verify', staging_ref,
        check=False,
    ).returncode

    dirty.unlink()
    harness.controller.reconcile()

    assert harness.request['status'] == 'cancelled'
    assert git(harness.repo, 'rev-parse', 'refs/heads/main') == harness.base_head
    assert not (harness.repo / 'job.txt').exists()
    assert not staging_worktree.exists()
    assert git(harness.repo, 'show-ref', '--verify', change_ref, check=False).returncode


def test_remove_cancellation_publishes_owned_tombstone_before_deletion(
    merge_harness,
):
    harness = merge_harness()
    dirty = harness.repo / 'keep-dirty.txt'
    dirty.write_text('local work\n', encoding='utf-8')
    harness.controller.reconcile()
    request = harness.request
    assert request['status'] == 'staged'

    cancelled = cancel_merge_job(harness.meta, remove=True)

    status = json.loads(Path(request['status_file']).read_text(encoding='utf-8'))
    assert cancelled['status'] == 'cancelled'
    assert status['state'] == status['stage'] == 'cancelled'
    assert status['cancelled'] is True
    assert status['submission_id'] == request['spec']['submission_id']

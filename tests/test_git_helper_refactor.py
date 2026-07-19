import subprocess

import pytest

from taskq import git as shared_git
from taskq.backends import git_ref
from taskq.backends.base import BackendError
from taskq.explore import git as explore_git
from taskq.integration import (
    fast_forward_checked_out,
    target_integration_lock_path,
)


def run_git(repo, *args):
    return subprocess.run(
        ['git', '-C', str(repo), *args],
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()


def init_repo(path):
    path.mkdir()
    run_git(path, 'init')
    run_git(path, 'config', 'user.email', 'taskq@example.test')
    run_git(path, 'config', 'user.name', 'Taskq Tests')
    run_git(path, 'config', 'commit.gpgsign', 'false')
    (path / 'tracked.txt').write_text('initial\n', encoding='utf-8')
    run_git(path, 'add', '.')
    run_git(path, 'commit', '-m', 'initial')


def test_explore_reexports_shared_policy_free_helpers():
    assert explore_git.repository is shared_git.repository
    assert explore_git.repository_root is shared_git.repository_root
    assert explore_git.changed_paths is shared_git.changed_paths
    assert explore_git.snapshot is shared_git.snapshot
    assert explore_git.diff is shared_git.diff
    assert explore_git.merge_ff is shared_git.merge_ff
    assert explore_git.rebase is shared_git.rebase
    assert explore_git.abort_rebase is shared_git.abort_rebase
    assert explore_git.begin_no_ff_merge is shared_git.begin_no_ff_merge
    assert explore_git.merge_in_progress is shared_git.merge_in_progress
    assert explore_git.complete_merge is shared_git.complete_merge


def test_explore_git_preserves_check_false_string_result(tmp_path):
    repo = tmp_path / 'repo'
    init_repo(repo)

    result = explore_git.git(
        repo, 'show-ref', '--verify', 'refs/heads/missing', check=False)

    assert result == ''


def test_explore_require_clean_keeps_explore_specific_error(tmp_path):
    repo = tmp_path / 'repo'
    init_repo(repo)
    (repo / 'untracked.txt').write_text('dirty\n', encoding='utf-8')

    with pytest.raises(BackendError, match='tq explore requires a clean'):
        explore_git.require_clean(repo)


def test_path_collisions_honor_git_case_folding():
    left = ['cache/Result.TXT']
    right = ['CACHE/result.txt']

    assert shared_git.path_collisions(left, right) == []
    assert shared_git.path_collisions(
        left, right, ignore_case=True,
    ) == ['CACHE/result.txt', 'cache/Result.TXT']
    assert shared_git.path_collisions(
        ['Build/Generated.bin'], ['build'], ignore_case=True,
    ) == ['Build/Generated.bin', 'build']


def test_target_integration_lock_is_shared_across_repo_worktrees(tmp_path):
    repo = tmp_path / 'repo'
    linked = tmp_path / 'linked'
    init_repo(repo)
    run_git(repo, 'worktree', 'add', '--detach', str(linked), 'HEAD')

    root_path = target_integration_lock_path(repo, 'refs/heads/main')
    linked_path = target_integration_lock_path(linked, 'main')

    assert root_path == linked_path
    assert root_path.parent == repo / '.git' / 'taskq-locks'


def test_shared_checked_out_fast_forward_helper(tmp_path):
    repo = tmp_path / 'repo'
    candidate = tmp_path / 'candidate'
    init_repo(repo)
    base = run_git(repo, 'rev-parse', 'HEAD')
    run_git(repo, 'worktree', 'add', '--detach', str(candidate), base)
    (candidate / 'candidate.txt').write_text('candidate\n', encoding='utf-8')
    run_git(candidate, 'add', 'candidate.txt')
    run_git(candidate, 'commit', '-m', 'candidate')
    head = run_git(candidate, 'rev-parse', 'HEAD')

    landed = fast_forward_checked_out(repo, base, head)

    assert landed == head
    assert run_git(repo, 'rev-parse', 'HEAD') == head
    assert (repo / 'candidate.txt').read_text(encoding='utf-8') == 'candidate\n'


def test_shared_fast_forward_never_overwrites_ignored_local_file(tmp_path):
    repo = tmp_path / 'repo'
    candidate = tmp_path / 'candidate'
    init_repo(repo)
    base = run_git(repo, 'rev-parse', 'HEAD')
    run_git(repo, 'worktree', 'add', '--detach', str(candidate), base)
    (candidate / 'generated.txt').write_text('incoming\n', encoding='utf-8')
    run_git(candidate, 'add', '-f', 'generated.txt')
    run_git(candidate, 'commit', '-m', 'track generated file')
    head = run_git(candidate, 'rev-parse', 'HEAD')
    (repo / '.git' / 'info' / 'exclude').write_text(
        'generated.txt\n', encoding='utf-8')
    local = repo / 'generated.txt'
    local.write_text('precious local bytes\n', encoding='utf-8')

    with pytest.raises(BackendError):
        shared_git.merge_ff(repo, head)

    assert run_git(repo, 'rev-parse', 'HEAD') == base
    assert local.read_text(encoding='utf-8') == 'precious local bytes\n'


def test_backend_checkout_uses_shared_worktree_primitives(
    monkeypatch, tmp_path,
):
    worktree = tmp_path / 'worktree'
    calls = []
    monkeypatch.setattr(
        git_ref.git_utils,
        'add_detached_worktree',
        lambda root, path, commit: calls.append(
            ('add', root, path, commit)),
    )
    monkeypatch.setattr(
        git_ref.git_utils,
        'remove_worktree',
        lambda root, path, force=True: calls.append(
            ('remove', root, path, force)) or True,
    )

    git_ref.create_worktree('/repo', 'abc123', worktree)
    git_ref.remove_worktree({
        'git_root': '/repo',
        'git_worktree': str(worktree),
        'workspace_owner': 'job',
    })

    assert calls == [
        ('add', '/repo', worktree, 'abc123'),
        ('remove', '/repo', str(worktree), True),
    ]

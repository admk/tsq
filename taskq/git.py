"""Shared, policy-free Git operations used by taskq workflows.

The helpers in this module deliberately keep workflow policy out of Git
plumbing.  In particular, callers decide whether a dirty checkout should
block an operation and which taskq-owned refs/worktrees may be removed.
"""

import os
import shutil
import subprocess
import unicodedata
from pathlib import Path

from .backends.base import BackendError


def git(cwd, *args, check=True, input_text=None, env=None):
    """Run Git in *cwd* and return stripped stdout.

    With ``check=False`` the returned object is ``CompletedProcess`` so the
    caller can inspect both the return code and diagnostics.
    """
    command = ['git', '-C', str(cwd), *map(str, args)]
    try:
        result = subprocess.run(
            command,
            input=input_text,
            capture_output=True,
            check=False,
            text=True,
            env=env,
        )
    except FileNotFoundError as error:
        raise BackendError('git command not found') from error
    if check and result.returncode:
        message = (result.stderr or result.stdout or '').strip()
        raise BackendError(message or 'git command failed')
    return result.stdout.strip() if check else result


def repository_root(cwd):
    return str(Path(git(cwd, 'rev-parse', '--show-toplevel')).resolve())


def common_dir(cwd):
    root = Path(repository_root(cwd))
    value = Path(git(root, 'rev-parse', '--git-common-dir'))
    if not value.is_absolute():
        value = root / value
    return str(value.resolve())


def repository(cwd, require_branch=True):
    root = repository_root(cwd)
    result = git(root, 'symbolic-ref', '--quiet', '--short', 'HEAD', check=False)
    branch = result.stdout.strip() if result.returncode == 0 else None
    if require_branch and not branch:
        raise BackendError('detached HEAD has no current branch')
    return root, branch, git(root, 'rev-parse', 'HEAD')


def resolve_commit(cwd, ref='HEAD'):
    return git(cwd, 'rev-parse', '--verify', '{}^{{commit}}'.format(ref))


def symbolic_ref(cwd, ref):
    """Return the symbolic target of *ref*, or ``None`` for a direct ref."""
    result = git(cwd, 'symbolic-ref', '--quiet', ref, check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def _ref_identity(ref):
    return unicodedata.normalize('NFC', str(ref)).casefold()


def local_branch_ref(cwd, branch):
    """Validate a literal local branch name and return its short/full names."""
    if not isinstance(branch, str) or not branch.strip():
        raise BackendError('target branch must be a non-empty local branch name')
    branch = branch.strip()
    if branch.startswith('refs/heads/'):
        short = branch[len('refs/heads/'):]
    else:
        short = branch
    result = git(cwd, 'check-ref-format', '--branch', short, check=False)
    if result.returncode or result.stdout.strip() != short:
        raise BackendError('invalid local target branch {!r}'.format(branch))
    ref = 'refs/heads/{}'.format(short)
    local_refs = git(
        cwd, 'for-each-ref', '--format=%(refname)', 'refs/heads/').splitlines()
    exists = git(cwd, 'show-ref', '--verify', '--quiet', ref, check=False)
    if exists.returncode == 0 and ref not in local_refs:
        raise BackendError(
            'local target branch {!r} is a noncanonical alias for an existing '
            'branch'.format(short))
    folded_ref = _ref_identity(ref)
    collisions = sorted(
        actual for actual in local_refs
        if actual != ref
        and _ref_identity(actual) == folded_ref
    )
    if collisions:
        names = ', '.join(
            actual[len('refs/heads/'):] for actual in collisions)
        raise BackendError(
            'local target branch {!r} has a noncanonical name collision with '
            '{}'.format(short, names))
    symbolic_target = symbolic_ref(cwd, ref)
    if symbolic_target:
        raise BackendError(
            'symbolic local target branch {!r} is not supported (points to {})'
            .format(short, symbolic_target))
    return short, ref


def local_branch(cwd, branch):
    """Validate and return ``(short_name, full_ref, head)``."""
    short, ref = local_branch_ref(cwd, branch)
    check = git(cwd, 'show-ref', '--verify', '--quiet', ref, check=False)
    if check.returncode:
        raise BackendError('local target branch does not exist: {}'.format(short))
    head = resolve_commit(cwd, ref)
    # Recheck after resolving so a concurrent direct-ref -> symref replacement
    # cannot be silently accepted by this validation path.
    local_branch_ref(cwd, short)
    return short, ref, head


def ensure_local_branch(cwd, branch, start_point, message=None):
    """Return a local branch, atomically creating it at *start_point*.

    Supplying the all-zero old object ID makes ``update-ref`` a create-only
    compare-and-swap. An identical concurrent create is accepted; a branch
    concurrently created at another commit produces an error.
    """
    short, ref = local_branch_ref(cwd, branch)
    check = git(cwd, 'show-ref', '--verify', '--quiet', ref, check=False)
    if check.returncode == 0:
        head = resolve_commit(cwd, ref)
        local_branch_ref(cwd, short)
        return short, ref, head
    start = resolve_commit(cwd, start_point)
    zero = '0' * len(start)
    try:
        update_ref(
            cwd, ref, start, old=zero, message=message, no_deref=True)
    except BackendError as error:
        # An identical concurrent create is benign. A different winner must
        # be surfaced: silently accepting it would violate the promise that a
        # newly-created destination starts at the submission-time HEAD.
        check = git(cwd, 'show-ref', '--verify', '--quiet', ref, check=False)
        if check.returncode:
            raise
        local_branch_ref(cwd, short)
        actual = resolve_commit(cwd, ref)
        if actual != start:
            raise BackendError(
                'local target branch {} was concurrently created at {}; '
                'expected {}'.format(short, actual[:12], start[:12])
            ) from error
    head = resolve_commit(cwd, ref)
    local_branch_ref(cwd, short)
    return short, ref, head


def current_branch(cwd):
    result = git(cwd, 'symbolic-ref', '--quiet', '--short', 'HEAD', check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def is_ancestor(cwd, ancestor, descendant):
    result = git(
        cwd, 'merge-base', '--is-ancestor', ancestor, descendant, check=False)
    return result.returncode == 0


def status(cwd):
    return git(cwd, 'status', '--porcelain', '--untracked-files=normal')


def is_clean(cwd):
    return not status(cwd)


def changed_paths(cwd, base='HEAD'):
    tracked = git(cwd, 'diff', '--name-only', base)
    untracked = git(cwd, 'ls-files', '--others', '--exclude-standard')
    return [line for line in (tracked + '\n' + untracked).splitlines() if line]


def changed_paths_between(cwd, base, head):
    output = git(
        cwd, 'diff', '--name-only', '-z', '--no-ext-diff', base, head)
    return [path for path in output.split('\0') if path]


def ignored_untracked_paths(cwd):
    output = git(
        cwd, 'ls-files', '--others', '--ignored', '--exclude-standard', '-z')
    return [path for path in output.split('\0') if path]


def core_ignorecase(cwd):
    """Return Git's case-folding policy for paths in this worktree."""
    result = git(
        cwd, 'config', '--bool', '--get', 'core.ignorecase', check=False)
    return result.returncode == 0 and result.stdout.strip().lower() == 'true'


def path_collisions(left, right, ignore_case=False):
    """Return paths colliding by equality or file/directory ancestry."""
    def indexed(paths):
        values = {}
        for path in paths:
            path = path.strip('/')
            key = path.casefold() if ignore_case else path
            values.setdefault(key, set()).add(path)
        return values

    def ancestors(path):
        return (path[:index] for index, value in enumerate(path) if value == '/')

    left = indexed(left)
    right = indexed(right)
    collisions = set()
    for key in left.keys() & right.keys():
        collisions.update(left[key])
        collisions.update(right[key])
    for key, paths in left.items():
        for ancestor in ancestors(key):
            if ancestor in right:
                collisions.update(paths)
                collisions.update(right[ancestor])
    for key, paths in right.items():
        for ancestor in ancestors(key):
            if ancestor in left:
                collisions.update(paths)
                collisions.update(left[ancestor])
    return sorted(path for path in collisions if path)


def snapshot(cwd, message):
    """Commit the current tracked and non-ignored untracked state."""
    if is_clean(cwd):
        return resolve_commit(cwd), False
    git(cwd, 'add', '-A')
    git(
        cwd, '-c', 'user.name=taskq', '-c', 'user.email=taskq@localhost',
        'commit', '--no-gpg-sign', '--no-verify', '-m', message)
    return resolve_commit(cwd), True


def synthetic_change_commit(cwd, source_base, message):
    """Create one immutable ``source_base -> final tree`` change commit.

    The job may have made commits of its own.  Only its final tree is used, so
    its internal topology and unrelated source history cannot be imported into
    the destination lane.  ``git add -A`` includes tracked changes and
    non-ignored untracked files while leaving ignored files out.
    """
    source_base = resolve_commit(cwd, source_base)
    git(cwd, 'add', '-A')
    result_tree = git(cwd, 'write-tree')
    source_tree = git(cwd, 'rev-parse', '{}^{{tree}}'.format(source_base))
    if result_tree == source_tree:
        return None, result_tree
    commit = git(
        cwd,
        '-c', 'user.name=taskq', '-c', 'user.email=taskq@localhost',
        'commit-tree', result_tree, '-p', source_base, '-F', '-',
        input_text=message,
    )
    return commit, result_tree


def diff(cwd, base, head='HEAD', limit=50000):
    text = git(cwd, 'diff', '--no-ext-diff', '--binary', '{}..{}'.format(base, head))
    return text if len(text) <= limit else text[:limit] + '\n[diff truncated]\n'


def update_ref(cwd, ref, new, old=None, message=None, no_deref=False):
    """Atomically update a ref, optionally requiring it to remain direct.

    The direct-ref path uses a prepared Git reference transaction. This holds
    the ref lock while its type is inspected, closing the check/update race in
    which a symbolic ref with the same peeled OID could otherwise pass Git's
    ordinary ``--no-deref`` old-value comparison.
    """
    if no_deref:
        return _update_direct_ref(cwd, ref, new, old=old, message=message)
    args = ['update-ref']
    if message:
        args += ['-m', message]
    args += [ref, new]
    if old is not None:
        args.append(old)
    git(cwd, *args)
    return resolve_commit(cwd, ref)


def _update_direct_ref(cwd, ref, new, old=None, message=None):
    command = ['git', '-C', str(cwd), 'update-ref']
    if message:
        command += ['-m', str(message)]
    command.append('--stdin')
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as error:
        raise BackendError('git command not found') from error

    def send(value, expected=None):
        try:
            process.stdin.write(value + '\n')
            process.stdin.flush()
        except (BrokenPipeError, OSError) as error:
            detail = process.stderr.read().strip()
            process.wait()
            raise BackendError(detail or 'git update-ref transaction failed') from error
        if expected is None:
            return
        response = process.stdout.readline().strip()
        if response != expected:
            process.stdin.close()
            detail = process.stderr.read().strip()
            process.wait()
            raise BackendError(
                detail or 'git update-ref transaction failed: expected {}, got {}'
                .format(expected, response or '<no response>'))

    finished = False
    try:
        send('option no-deref')
        send('start', 'start: ok')
        values = ['update', str(ref), str(new)]
        if old is not None:
            values.append(str(old))
        send(' '.join(values))
        send('prepare', 'prepare: ok')
        target = symbolic_ref(cwd, ref)
        if target:
            send('abort', 'abort: ok')
            finished = True
            raise BackendError(
                'ref {} became symbolic (points to {}); update refused'.format(
                    ref, target))
        old_text = '' if old is None else str(old)
        creating = bool(old_text) and old_text.strip('0') == ''
        actual_refs = git(
            cwd, 'for-each-ref', '--format=%(refname)',
            'refs/heads/',
        ).splitlines()
        collisions = [
            actual for actual in actual_refs
            if actual != ref and _ref_identity(actual) == _ref_identity(ref)
        ]
        if ref.startswith('refs/heads/') and collisions:
            send('abort', 'abort: ok')
            finished = True
            raise BackendError(
                'ref {} has a noncanonical name collision with {}; update '
                'refused'.format(ref, ', '.join(sorted(collisions))))
        if not creating and ref.startswith('refs/heads/') and ref not in actual_refs:
            send('abort', 'abort: ok')
            finished = True
            raise BackendError(
                'ref {} is no longer a byte-exact local branch; update '
                'refused'.format(ref))
        send('commit', 'commit: ok')
        finished = True
    except Exception:
        if not finished and process.poll() is None:
            try:
                send('abort', 'abort: ok')
            except (BackendError, BrokenPipeError, OSError):
                pass
        raise
    finally:
        if process.stdin and not process.stdin.closed:
            process.stdin.close()
        if process.poll() is None:
            process.wait()
    if process.returncode:
        detail = process.stderr.read().strip()
        raise BackendError(detail or 'git update-ref transaction failed')
    return resolve_commit(cwd, ref)


def delete_ref(cwd, ref, old=None):
    args = ['update-ref', '-d', ref]
    if old is not None:
        args.append(old)
    result = git(cwd, *args, check=False)
    return result.returncode == 0


def worktrees(cwd):
    """Return parsed ``git worktree list --porcelain`` records."""
    output = git(cwd, 'worktree', 'list', '--porcelain', '-z')
    records = []
    for raw in output.split('\0\0'):
        fields = [field for field in raw.split('\0') if field]
        if not fields:
            continue
        record = {}
        for field in fields:
            key, _, value = field.partition(' ')
            if key in {'bare', 'detached', 'locked', 'prunable'}:
                record[key] = value or True
            else:
                record[key] = value
        if record.get('worktree'):
            records.append(record)
    return records


def _path_identity(path):
    return os.path.normcase(str(Path(path).expanduser().resolve()))


def _worktree_admin_dirs(cwd, records):
    """Map porcelain worktree paths to their per-worktree Git directories."""
    common = Path(common_dir(cwd))
    mapping = {}
    if records and not records[0].get('bare'):
        mapping[_path_identity(records[0]['worktree'])] = common
    linked = common / 'worktrees'
    try:
        entries = list(linked.iterdir())
    except OSError:
        entries = []
    for admin in entries:
        try:
            pointer = (admin / 'gitdir').read_text(encoding='utf-8').strip()
        except OSError:
            continue
        git_file = Path(pointer)
        if not git_file.is_absolute():
            git_file = admin / git_file
        mapping[_path_identity(git_file.parent)] = admin
    return mapping


def _worktree_reserved_branches(git_dir):
    """Return branches reserved by detached rebase/bisect operations."""
    git_dir = Path(git_dir)
    values = []
    for relative in (
        Path('rebase-merge') / 'head-name',
        Path('rebase-apply') / 'head-name',
        Path('BISECT_START'),
    ):
        try:
            value = (git_dir / relative).read_text(encoding='utf-8').strip()
        except OSError:
            continue
        if value.startswith('refs/heads/'):
            values.append(value)
        elif value and value != 'HEAD' and not value.startswith('refs/'):
            values.append('refs/heads/{}'.format(value))
    return values


def branch_worktrees(cwd, target_ref):
    target_folded = _ref_identity(target_ref)
    matches = []
    records = worktrees(cwd)
    admin_dirs = _worktree_admin_dirs(cwd, records)
    for item in records:
        branches = [item.get('branch')]
        if item.get('detached'):
            git_dir = admin_dirs.get(_path_identity(item['worktree']))
            if git_dir is None:
                result = git(
                    item['worktree'], 'rev-parse', '--absolute-git-dir',
                    check=False,
                )
                if result.returncode == 0 and result.stdout.strip():
                    git_dir = Path(result.stdout.strip())
            if git_dir is not None:
                branches.extend(_worktree_reserved_branches(git_dir))
        if any(
            branch and _ref_identity(branch) == target_folded
            for branch in branches
        ):
            matches.append(item)
    return matches


def add_detached_worktree(cwd, path, start_point):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    git(cwd, 'worktree', 'add', '--detach', str(path), start_point)
    return str(path)


def remove_worktree(cwd, path, force=True):
    args = ['worktree', 'remove']
    if force:
        args.append('--force')
    args.append(str(path))
    result = git(cwd, *args, check=False)
    if result.returncode:
        shutil.rmtree(path, ignore_errors=True)
        git(cwd, 'worktree', 'prune', check=False)
        return False
    return True


def reset_hard(cwd, commit):
    git(cwd, 'reset', '--hard', commit)
    git(cwd, 'clean', '-fd')
    return resolve_commit(cwd)


def cherry_pick(cwd, commit):
    result = git(
        cwd,
        '-c', 'user.name=taskq', '-c', 'user.email=taskq@localhost',
        '-c', 'commit.gpgSign=false', '-c', 'core.hooksPath=/dev/null',
        'cherry-pick', '--no-gpg-sign', commit, check=False,
    )
    output = (result.stdout + result.stderr).strip()
    return result.returncode == 0, output


def cherry_pick_in_progress(cwd):
    path = Path(git(cwd, 'rev-parse', '--git-path', 'CHERRY_PICK_HEAD'))
    if not path.is_absolute():
        path = Path(cwd) / path
    return path.exists()


def abort_cherry_pick(cwd):
    if cherry_pick_in_progress(cwd):
        git(cwd, 'cherry-pick', '--abort', check=False)


def merge_ff(cwd, commit):
    git(
        cwd,
        '-c', 'commit.gpgSign=false', '-c', 'core.hooksPath=/dev/null',
        'merge', '--ff-only', '--no-edit', '--no-verify',
        '--no-overwrite-ignore', commit)
    return resolve_commit(cwd)


def atomic_write(path, text):
    """Atomically replace a UTF-8 text file in its existing filesystem."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name('.{}.{}.tmp'.format(path.name, os.getpid()))
    temporary.write_text(text, encoding='utf-8')
    os.replace(str(temporary), str(path))

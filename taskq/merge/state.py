"""Durable, repository-wide state for standalone merge queues."""

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


SCHEMA_VERSION = 1
TERMINAL_REQUEST_STATES = frozenset({'landed', 'failed', 'cancelled'})
ACTIVE_REQUEST_STATES = frozenset({
    'waiting', 'queued', 'applying', 'resolving', 'staged',
})

_JSON_COLUMNS = frozenset({'spec', 'backend', 'result'})

_SCHEMA = r"""
CREATE TABLE lanes (
    id TEXT PRIMARY KEY,
    repo_root TEXT NOT NULL,
    common_dir TEXT NOT NULL,
    target_branch TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    staging_ref TEXT NOT NULL,
    staging_worktree TEXT NOT NULL,
    base_head TEXT,
    staging_head TEXT,
    next_sequence INTEGER NOT NULL DEFAULT 1,
    needs_rebuild INTEGER NOT NULL DEFAULT 1,
    blocked_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(common_dir, target_ref)
);

CREATE TABLE requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lane_id TEXT NOT NULL REFERENCES lanes(id),
    sequence INTEGER,
    job_key TEXT NOT NULL UNIQUE,
    parent_job_id INTEGER NOT NULL,
    job_dir TEXT NOT NULL,
    meta_file TEXT,
    command_result_file TEXT NOT NULL,
    status_file TEXT NOT NULL,
    result_file TEXT NOT NULL,
    source_worktree TEXT NOT NULL,
    source_base TEXT NOT NULL,
    change_ref TEXT,
    change_head TEXT,
    result_tree TEXT,
    staged_head TEXT,
    status TEXT NOT NULL DEFAULT 'waiting',
    resolver_token TEXT,
    resolver_job_id INTEGER,
    resolver_attempts INTEGER NOT NULL DEFAULT 0,
    spec TEXT NOT NULL DEFAULT '{}',
    backend TEXT NOT NULL DEFAULT '{}',
    result TEXT,
    error TEXT,
    ready_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE(lane_id, sequence)
);

CREATE INDEX requests_lane_queue_idx
    ON requests(lane_id, status, sequence, id);
CREATE INDEX requests_active_idx
    ON requests(status, updated_at);
"""


def timestamp(value=None):
    if isinstance(value, str):
        return value
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds')


def _dump(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)


def _row(value):
    if value is None:
        return None
    result = dict(value)
    for key in _JSON_COLUMNS & result.keys():
        if result[key] is not None:
            result[key] = json.loads(result[key])
    for key in ('needs_rebuild',):
        if key in result:
            result[key] = bool(result[key])
    return result


class MergeState:
    """SQLite FIFO shared by all taskq queues for one Git common dir."""

    def __init__(self, path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(
            str(self.path), timeout=30, isolation_level=None,
            check_same_thread=False,
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute('PRAGMA foreign_keys = ON')
        self._db.execute('PRAGMA busy_timeout = 30000')
        self._db.execute('PRAGMA journal_mode = WAL')
        self._db.execute('PRAGMA synchronous = FULL')
        self._initialize()

    def _initialize(self):
        version = self._db.execute('PRAGMA user_version').fetchone()[0]
        if version > SCHEMA_VERSION:
            raise RuntimeError(
                'merge state schema {} is newer than supported schema {}'.format(
                    version, SCHEMA_VERSION))
        if version == 0:
            self._db.executescript(_SCHEMA)
            self._db.execute('PRAGMA user_version = {}'.format(SCHEMA_VERSION))
        elif version < SCHEMA_VERSION:
            raise RuntimeError('no migration available for schema {}'.format(version))

    @contextmanager
    def transaction(self):
        with self._lock:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                yield self._db
            except Exception:
                self._db.rollback()
                raise
            else:
                self._db.commit()

    def close(self):
        with self._lock:
            self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def _one(self, sql, params=()):
        with self._lock:
            return _row(self._db.execute(sql, params).fetchone())

    def _all(self, sql, params=()):
        with self._lock:
            return [_row(row) for row in self._db.execute(sql, params)]

    def ensure_lane(self, lane, now=None):
        now = timestamp(now)
        with self.transaction() as db:
            db.execute(
                'INSERT INTO lanes '
                '(id, repo_root, common_dir, target_branch, target_ref, '
                'staging_ref, staging_worktree, created_at, updated_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(id) DO NOTHING',
                (
                    lane['id'], lane['repo_root'], lane['common_dir'],
                    lane['target_branch'], lane['target_ref'],
                    lane['staging_ref'], lane['staging_worktree'], now, now,
                ),
            )
            existing = _row(db.execute(
                'SELECT * FROM lanes WHERE id = ?', (lane['id'],)
            ).fetchone())
            identity = ('repo_root', 'common_dir', 'target_branch', 'target_ref')
            if any(existing[key] != lane[key] for key in identity):
                raise ValueError('merge lane identity collision: {}'.format(lane['id']))
            return existing

    def get_lane(self, lane_id):
        return self._one('SELECT * FROM lanes WHERE id = ?', (lane_id,))

    def list_lanes(self, active_only=False):
        if not active_only:
            return self._all('SELECT * FROM lanes ORDER BY created_at, id')
        placeholders = ','.join('?' for _ in ACTIVE_REQUEST_STATES)
        return self._all(
            'SELECT DISTINCT l.* FROM lanes l JOIN requests r ON r.lane_id=l.id '
            'WHERE r.status IN ({}) ORDER BY l.created_at, l.id'.format(placeholders),
            tuple(sorted(ACTIVE_REQUEST_STATES)),
        )

    def update_lane(self, lane_id, **changes):
        allowed = {
            'base_head', 'staging_head', 'needs_rebuild', 'blocked_reason',
            'staging_worktree',
        }
        unknown = set(changes) - allowed
        if unknown:
            raise ValueError('unsupported lane fields: {}'.format(', '.join(unknown)))
        if not changes:
            return self.get_lane(lane_id)
        changes['updated_at'] = timestamp()
        if 'needs_rebuild' in changes:
            changes['needs_rebuild'] = int(bool(changes['needs_rebuild']))
        assignments = ', '.join('{} = ?'.format(key) for key in changes)
        values = list(changes.values()) + [lane_id]
        with self.transaction() as db:
            db.execute('UPDATE lanes SET {} WHERE id = ?'.format(assignments), values)
            return _row(db.execute(
                'SELECT * FROM lanes WHERE id = ?', (lane_id,)
            ).fetchone())

    def add_request(self, request, now=None):
        now = timestamp(now)
        with self.transaction() as db:
            db.execute(
                'INSERT INTO requests '
                '(lane_id, job_key, parent_job_id, job_dir, meta_file, '
                'command_result_file, status_file, result_file, source_worktree, '
                'source_base, spec, backend, created_at, updated_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(job_key) DO NOTHING',
                (
                    request['lane_id'], request['job_key'],
                    int(request['parent_job_id']), request['job_dir'],
                    request.get('meta_file'), request['command_result_file'],
                    request['status_file'], request['result_file'],
                    request['source_worktree'], request['source_base'],
                    _dump(request['spec']), _dump(request['backend']), now, now,
                ),
            )
            row = _row(db.execute(
                'SELECT * FROM requests WHERE job_key = ?',
                (request['job_key'],),
            ).fetchone())
            if row['lane_id'] != request['lane_id']:
                raise ValueError('merge job identity collision: {}'.format(
                    request['job_key']))
            return row

    def get_request(self, request_id):
        return self._one('SELECT * FROM requests WHERE id = ?', (int(request_id),))

    def get_request_by_job_key(self, job_key):
        return self._one('SELECT * FROM requests WHERE job_key = ?', (job_key,))

    def list_requests(self, lane_id=None, statuses=None, include_terminal=True):
        where = []
        params = []
        if lane_id is not None:
            where.append('lane_id = ?')
            params.append(lane_id)
        if statuses is not None:
            statuses = tuple(statuses)
            if not statuses:
                return []
            where.append('status IN ({})'.format(','.join('?' for _ in statuses)))
            params.extend(statuses)
        elif not include_terminal:
            where.append('status IN ({})'.format(
                ','.join('?' for _ in ACTIVE_REQUEST_STATES)))
            params.extend(sorted(ACTIVE_REQUEST_STATES))
        clause = ' WHERE ' + ' AND '.join(where) if where else ''
        return self._all(
            'SELECT * FROM requests{} '
            'ORDER BY CASE WHEN sequence IS NULL THEN 1 ELSE 0 END, sequence, id'.format(
                clause),
            tuple(params),
        )

    def mark_ready(self, request_id, change_ref, change_head, result_tree, now=None):
        """Atomically assign the next lane FIFO sequence at command handoff."""
        now = timestamp(now)
        with self.transaction() as db:
            request = db.execute(
                'SELECT * FROM requests WHERE id = ?', (int(request_id),)
            ).fetchone()
            if request is None:
                return None
            if request['status'] != 'waiting':
                return _row(request)
            lane = db.execute(
                'SELECT * FROM lanes WHERE id = ?', (request['lane_id'],)
            ).fetchone()
            sequence = int(lane['next_sequence'])
            db.execute(
                'UPDATE lanes SET next_sequence=?, updated_at=? WHERE id=?',
                (sequence + 1, now, request['lane_id']),
            )
            status_value = 'queued' if change_head else 'landed'
            completed_at = now if not change_head else None
            result = {'noop': True} if not change_head else None
            db.execute(
                'UPDATE requests SET sequence=?, change_ref=?, change_head=?, '
                'result_tree=?, status=?, ready_at=?, updated_at=?, completed_at=?, '
                'result=? WHERE id=?',
                (
                    sequence, change_ref, change_head, result_tree,
                    status_value, now, now, completed_at,
                    _dump(result) if result is not None else None,
                    int(request_id),
                ),
            )
            return _row(db.execute(
                'SELECT * FROM requests WHERE id = ?', (int(request_id),)
            ).fetchone())

    def update_request(self, request_id, **changes):
        allowed = {
            'status', 'change_ref', 'change_head', 'result_tree', 'staged_head',
            'resolver_token', 'resolver_job_id', 'resolver_attempts', 'result',
            'error', 'completed_at',
        }
        unknown = set(changes) - allowed
        if unknown:
            raise ValueError('unsupported request fields: {}'.format(
                ', '.join(sorted(unknown))))
        if not changes:
            return self.get_request(request_id)
        changes['updated_at'] = timestamp()
        for key in _JSON_COLUMNS & changes.keys():
            if changes[key] is not None:
                changes[key] = _dump(changes[key])
        assignments = ', '.join('{} = ?'.format(key) for key in changes)
        values = list(changes.values()) + [int(request_id)]
        with self.transaction() as db:
            db.execute(
                'UPDATE requests SET {} WHERE id = ?'.format(assignments), values)
            return _row(db.execute(
                'SELECT * FROM requests WHERE id = ?', (int(request_id),)
            ).fetchone())

    def finish_request(self, request_id, status_value, result=None, error=None):
        if status_value not in TERMINAL_REQUEST_STATES:
            raise ValueError('invalid terminal merge status: {}'.format(status_value))
        request = self.get_request(request_id)
        if request is None or request['status'] in TERMINAL_REQUEST_STATES:
            return request
        merged_result = dict(request.get('result') or {})
        if result:
            merged_result.update(result)
        row = self.update_request(
            request_id,
            status=status_value,
            result=merged_result or None,
            error=error,
            completed_at=timestamp(),
        )
        if request['status'] in {'applying', 'resolving', 'staged'}:
            self.update_lane(request['lane_id'], needs_rebuild=True)
        return row

    def cancel_job(self, job_key):
        request = self.get_request_by_job_key(job_key)
        if request is None or request['status'] in TERMINAL_REQUEST_STATES:
            return request
        return self.finish_request(
            request['id'], 'cancelled', result={'cancelled': True})

    def active(self):
        placeholders = ','.join('?' for _ in ACTIVE_REQUEST_STATES)
        row = self._one(
            'SELECT COUNT(*) AS count FROM requests WHERE status IN ({})'.format(
                placeholders),
            tuple(sorted(ACTIVE_REQUEST_STATES)),
        )
        return int(row['count'])

    def backend_specs(self, active_only=False):
        requests = self.list_requests(include_terminal=not active_only)
        unique = {}
        for request in requests:
            backend = request['backend']
            key = backend.get('state_dir') or _dump(backend)
            unique[key] = backend
        return list(unique.values())

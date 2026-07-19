"""Durable project-wide state for autonomous exploration campaigns."""

import fcntl
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path


SCHEMA_VERSION = 3

_JSON_COLUMNS = {
    'budgets', 'config', 'metadata', 'payload', 'evidence',
    'memory_updates', 'next_direction', 'provenance', 'result',
}

_MERGE_HEAD_PREDECESSORS = {
    'fast_forwarding': {'snapshotting'},
    'landing': {'fast_forwarding', 'rebasing', 'merge_fallback', 'resolving'},
}
_MERGE_HEAD_DOWNSTREAM = {
    'fast_forwarding': {
        'fast_forwarding', 'rebasing', 'merge_fallback', 'resolving', 'landing',
    },
    'landing': {'landing'},
}

_SCHEMA = r"""
CREATE TABLE campaigns (
    id TEXT PRIMARY KEY,
    objective TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    target_head TEXT NOT NULL,
    mainline_ref TEXT NOT NULL,
    mainline_head TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    generation INTEGER NOT NULL DEFAULT 0,
    stall_count INTEGER NOT NULL DEFAULT 0,
    budgets TEXT NOT NULL DEFAULT '{}',
    config TEXT NOT NULL DEFAULT '{}',
    controller_id TEXT,
    heartbeat_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE directions (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    parent_id TEXT REFERENCES directions(id),
    fingerprint TEXT NOT NULL,
    hypothesis TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'planned',
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (campaign_id, fingerprint)
);

CREATE TABLE attempts (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    direction_id TEXT NOT NULL REFERENCES directions(id),
    branch TEXT NOT NULL,
    worktree TEXT NOT NULL,
    base_head TEXT NOT NULL,
    head TEXT NOT NULL,
    current_job_id TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    fix_count INTEGER NOT NULL DEFAULT 0,
    stale_count INTEGER NOT NULL DEFAULT 0,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    attempt_id TEXT REFERENCES attempts(id),
    direction_id TEXT REFERENCES directions(id),
    backend_job_id TEXT NOT NULL,
    role TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    terminal_at TEXT,
    inspected_at TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (campaign_id, backend_job_id)
);

CREATE TABLE terminal_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    job_id TEXT NOT NULL REFERENCES jobs(id),
    kind TEXT NOT NULL DEFAULT 'terminal',
    payload TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    claimed_by TEXT,
    claimed_at TEXT,
    claim_expires_at TEXT,
    completed_at TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (job_id, kind)
);

CREATE TABLE decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    attempt_id TEXT REFERENCES attempts(id),
    event_id INTEGER REFERENCES terminal_events(id),
    merge_request_id INTEGER,
    phase TEXT NOT NULL DEFAULT 'fix',
    generation INTEGER NOT NULL DEFAULT 0,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    evidence TEXT NOT NULL DEFAULT '[]',
    memory_updates TEXT NOT NULL DEFAULT '[]',
    next_direction TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    dedupe_key TEXT UNIQUE,
    created_at TEXT NOT NULL,
    UNIQUE (event_id)
);

CREATE TABLE merge_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    attempt_id TEXT NOT NULL REFERENCES attempts(id),
    head TEXT NOT NULL,
    accepted_seq INTEGER NOT NULL,
    accepted_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    claimed_by TEXT,
    claimed_at TEXT,
    claim_expires_at TEXT,
    completed_at TEXT,
    result TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    dedupe_key TEXT NOT NULL,
    UNIQUE (campaign_id, accepted_seq),
    UNIQUE (campaign_id, dedupe_key)
);

CREATE TABLE findings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT REFERENCES campaigns(id),
    attempt_id TEXT REFERENCES attempts(id),
    direction_id TEXT REFERENCES directions(id),
    claim TEXT NOT NULL,
    outcome TEXT,
    trust TEXT NOT NULL,
    confidence REAL,
    scope TEXT,
    source_commit TEXT,
    provenance TEXT NOT NULL DEFAULT '{}',
    metadata TEXT NOT NULL DEFAULT '{}',
    dedupe_key TEXT UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT REFERENCES campaigns(id),
    topic TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    dedupe_key TEXT UNIQUE,
    claimed_by TEXT,
    claimed_at TEXT,
    claim_expires_at TEXT,
    completed_at TEXT,
    error TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT REFERENCES campaigns(id),
    kind TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX directions_campaign_idx
    ON directions(campaign_id, created_at);
CREATE INDEX attempts_campaign_idx
    ON attempts(campaign_id, status, created_at);
CREATE INDEX jobs_campaign_idx
    ON jobs(campaign_id, status, created_at);
CREATE INDEX terminal_events_queue_idx
    ON terminal_events(campaign_id, status, id);
CREATE INDEX merge_requests_queue_idx
    ON merge_requests(campaign_id, status, accepted_seq);
CREATE INDEX findings_memory_idx
    ON findings(trust, created_at);
CREATE INDEX outbox_queue_idx
    ON outbox(status, id);
CREATE INDEX audit_campaign_idx
    ON audit_events(campaign_id, id);
"""


def _schema_statements():
    statement = []
    for line in _SCHEMA.splitlines():
        statement.append(line)
        sql = '\n'.join(statement).strip()
        if sql and sqlite3.complete_statement(sql):
            yield sql
            statement = []
    if any(line.strip() for line in statement):
        raise RuntimeError('incomplete explore state schema')


def _timestamp(value=None):
    if isinstance(value, str):
        return value
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds')


def _expiry(seconds, value=None):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return _timestamp(value + timedelta(seconds=seconds))


def _dump(value):
    return json.dumps(
        value, sort_keys=True, separators=(',', ':'), default=str)


def _row(value):
    if value is None:
        return None
    result = dict(value)
    for key in _JSON_COLUMNS & result.keys():
        if result[key] is not None:
            result[key] = json.loads(result[key])
    return result


class ExploreState:
    """SQLite state store shared by all campaigns in one repository."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(
            str(self.path), timeout=30, isolation_level=None,
            check_same_thread=False,
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute('PRAGMA foreign_keys = ON')
        self._db.execute('PRAGMA busy_timeout = 30000')
        try:
            self._initialize()
        except Exception:
            self._db.close()
            raise

    def _initialize(self):
        lock_path = self.path.with_name(self.path.name + '.init.lock')
        with open(lock_path, 'a', encoding='utf-8') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                self._db.execute('PRAGMA journal_mode = WAL')
                self._db.execute('PRAGMA synchronous = NORMAL')
                with self._transaction() as db:
                    version = db.execute('PRAGMA user_version').fetchone()[0]
                    if version > SCHEMA_VERSION:
                        raise RuntimeError(
                            'explore state schema {} is newer than supported '
                            'schema {}'.format(version, SCHEMA_VERSION))
                    if version == 0:
                        for statement in _schema_statements():
                            db.execute(statement)
                        db.execute(
                            'PRAGMA user_version = {}'.format(SCHEMA_VERSION))
                    elif version < SCHEMA_VERSION:
                        raise RuntimeError(
                            'explore state schema {} is unsupported; recreate '
                            'the campaign'.format(version))
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    @property
    def schema_version(self):
        with self._lock:
            return self._db.execute('PRAGMA user_version').fetchone()[0]

    @property
    def journal_mode(self):
        with self._lock:
            return self._db.execute('PRAGMA journal_mode').fetchone()[0]

    def close(self):
        with self._lock:
            self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    @contextmanager
    def _transaction(self):
        with self._lock:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                yield self._db
            except Exception:
                self._db.rollback()
                raise
            else:
                self._db.commit()

    def _fetchone(self, query, params=()):
        with self._lock:
            return _row(self._db.execute(query, params).fetchone())

    def _fetchall(self, query, params=()):
        with self._lock:
            return [_row(row) for row in self._db.execute(query, params)]

    @staticmethod
    def _audit(db, campaign_id, kind, payload, now):
        cursor = db.execute(
            'INSERT INTO audit_events (campaign_id, kind, payload, created_at) '
            'VALUES (?, ?, ?, ?)',
            (campaign_id, kind, _dump(payload), now),
        )
        return cursor.lastrowid

    @staticmethod
    def _outbox(db, campaign_id, topic, payload, dedupe_key, now):
        cursor = db.execute(
            'INSERT INTO outbox '
            '(campaign_id, topic, payload, dedupe_key, created_at) '
            'VALUES (?, ?, ?, ?, ?) ON CONFLICT(dedupe_key) DO NOTHING',
            (campaign_id, topic, _dump(payload), dedupe_key, now),
        )
        if cursor.rowcount:
            return cursor.lastrowid
        row = db.execute(
            'SELECT id FROM outbox WHERE dedupe_key = ?', (dedupe_key,)
        ).fetchone()
        return row['id']

    @staticmethod
    def _changed(changes, allowed, json_fields=()):
        unknown = set(changes) - set(allowed)
        if unknown:
            raise ValueError('unsupported fields: {}'.format(
                ', '.join(sorted(unknown))))
        return {
            key: _dump(value) if key in json_fields else value
            for key, value in changes.items()
        }

    def create_campaign(
        self, campaign_id, objective, target_ref, mainline_ref,
        target_head='', mainline_head=None, status='active', budgets=None,
        config=None, now=None,
    ):
        now = _timestamp(now)
        mainline_head = target_head if mainline_head is None else mainline_head
        with self._transaction() as db:
            db.execute(
                'INSERT INTO campaigns '
                '(id, objective, target_ref, target_head, mainline_ref, '
                'mainline_head, status, budgets, config, created_at, updated_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (campaign_id, objective, target_ref, target_head, mainline_ref,
                 mainline_head, status, _dump(budgets or {}),
                 _dump(config or {}), now, now),
            )
            self._audit(db, campaign_id, 'campaign.created', {}, now)
            return _row(db.execute(
                'SELECT * FROM campaigns WHERE id = ?', (campaign_id,)
            ).fetchone())

    def get_campaign(self, campaign_id):
        return self._fetchone(
            'SELECT * FROM campaigns WHERE id = ?', (campaign_id,))

    def list_campaigns(self, status=None, limit=None):
        query = 'SELECT * FROM campaigns'
        params = []
        if status is not None:
            query += ' WHERE status = ?'
            params.append(status)
        query += ' ORDER BY created_at DESC'
        if limit is not None:
            query += ' LIMIT ?'
            params.append(limit)
        return self._fetchall(query, params)

    def update_campaign(self, campaign_id, **changes):
        allowed = {
            'objective', 'target_ref', 'target_head', 'mainline_ref',
            'mainline_head', 'status', 'generation', 'stall_count', 'budgets',
            'config', 'controller_id', 'heartbeat_at', 'finished_at',
        }
        values = self._changed(changes, allowed, {'budgets', 'config'})
        if not values:
            return self.get_campaign(campaign_id)
        now = _timestamp()
        values['updated_at'] = now
        assignments = ', '.join('{} = ?'.format(key) for key in values)
        with self._transaction() as db:
            cursor = db.execute(
                'UPDATE campaigns SET {} WHERE id = ?'.format(assignments),
                list(values.values()) + [campaign_id],
            )
            if not cursor.rowcount:
                raise KeyError(campaign_id)
            self._audit(db, campaign_id, 'campaign.updated', changes, now)
            return _row(db.execute(
                'SELECT * FROM campaigns WHERE id = ?', (campaign_id,)
            ).fetchone())

    def delete_campaigns(self, campaign_ids):
        """Delete finished campaign history and all campaign-scoped memory."""
        campaign_ids = list(dict.fromkeys(campaign_ids))
        if not campaign_ids:
            return 0
        placeholders = ','.join('?' for _ in campaign_ids)
        where = 'campaign_id IN ({})'.format(placeholders)
        with self._transaction() as db:
            rows = db.execute(
                'SELECT id, status FROM campaigns WHERE id IN ({})'.format(
                    placeholders), campaign_ids).fetchall()
            unfinished = [
                row['id'] for row in rows
                if row['status'] not in {'completed', 'failed'}]
            if unfinished:
                raise ValueError(
                    'cannot delete active campaigns: {}'.format(
                        ', '.join(sorted(unfinished))))
            for table in (
                'decisions', 'findings', 'merge_requests',
                'terminal_events', 'jobs', 'attempts', 'directions',
                'outbox', 'audit_events',
            ):
                db.execute(
                    'DELETE FROM {} WHERE {}'.format(table, where), campaign_ids)
            db.execute(
                'DELETE FROM campaigns WHERE id IN ({})'.format(placeholders),
                campaign_ids)
            return len(rows)

    def heartbeat(self, campaign_id, controller_id=None, at=None):
        at = _timestamp(at)
        with self._transaction() as db:
            cursor = db.execute(
                'UPDATE campaigns SET heartbeat_at = ?, '
                'controller_id = COALESCE(?, controller_id), '
                'updated_at = ? WHERE id = ?',
                (at, controller_id, at, campaign_id),
            )
            if not cursor.rowcount:
                raise KeyError(campaign_id)
            return _row(db.execute(
                'SELECT * FROM campaigns WHERE id = ?', (campaign_id,)
            ).fetchone())

    def list_stale_campaigns(self, before, statuses=('active', 'draining')):
        if not statuses:
            return []
        placeholders = ','.join('?' for _ in statuses)
        query = (
            'SELECT * FROM campaigns WHERE status IN ({}) AND '
            '(heartbeat_at IS NULL OR heartbeat_at < ?) ORDER BY created_at'
        ).format(placeholders)
        return self._fetchall(query, list(statuses) + [_timestamp(before)])

    def add_direction(
        self, campaign_id, direction_id, hypothesis, fingerprint,
        parent_id=None, generation=0, status='planned', metadata=None, now=None,
    ):
        now = _timestamp(now)
        with self._transaction() as db:
            db.execute(
                'INSERT INTO directions '
                '(id, campaign_id, parent_id, fingerprint, hypothesis, '
                'generation, status, metadata, created_at, updated_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (direction_id, campaign_id, parent_id, fingerprint, hypothesis,
                 generation, status, _dump(metadata or {}), now, now),
            )
            self._audit(db, campaign_id, 'direction.added',
                        {'direction_id': direction_id}, now)
            return _row(db.execute(
                'SELECT * FROM directions WHERE id = ?', (direction_id,)
            ).fetchone())

    def get_direction(self, direction_id):
        return self._fetchone(
            'SELECT * FROM directions WHERE id = ?', (direction_id,))

    def list_directions(self, campaign_id=None, status=None):
        clauses, params = [], []
        if campaign_id is not None:
            clauses.append('campaign_id = ?')
            params.append(campaign_id)
        if status is not None:
            clauses.append('status = ?')
            params.append(status)
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        return self._fetchall(
            'SELECT * FROM directions{} ORDER BY created_at, id'.format(where),
            params,
        )

    def update_direction(self, direction_id, **changes):
        allowed = {
            'parent_id', 'fingerprint', 'hypothesis', 'generation', 'status',
            'metadata',
        }
        return self._update_entity(
            'directions', direction_id, changes, allowed, {'metadata'},
            'direction.updated')

    def add_attempt(
        self, campaign_id, attempt_id, direction_id, branch, worktree,
        base_head, head=None, status='active', current_job_id=None,
        fix_count=0, stale_count=0, metadata=None, now=None,
    ):
        now = _timestamp(now)
        head = base_head if head is None else head
        with self._transaction() as db:
            db.execute(
                'INSERT INTO attempts '
                '(id, campaign_id, direction_id, branch, worktree, base_head, '
                'head, current_job_id, status, fix_count, stale_count, '
                'metadata, created_at, updated_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (attempt_id, campaign_id, direction_id, branch, str(worktree),
                 base_head, head, current_job_id, status, fix_count,
                 stale_count, _dump(metadata or {}), now, now),
            )
            self._audit(db, campaign_id, 'attempt.added',
                        {'attempt_id': attempt_id}, now)
            return _row(db.execute(
                'SELECT * FROM attempts WHERE id = ?', (attempt_id,)
            ).fetchone())

    def get_attempt(self, attempt_id):
        return self._fetchone(
            'SELECT * FROM attempts WHERE id = ?', (attempt_id,))

    def list_attempts(
        self, campaign_id=None, direction_id=None, status=None,
    ):
        clauses, params = [], []
        for field, value in (
            ('campaign_id', campaign_id), ('direction_id', direction_id),
            ('status', status),
        ):
            if value is not None:
                clauses.append('{} = ?'.format(field))
                params.append(value)
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        return self._fetchall(
            'SELECT * FROM attempts{} ORDER BY created_at, id'.format(where),
            params,
        )

    def update_attempt(self, attempt_id, **changes):
        allowed = {
            'direction_id', 'branch', 'worktree', 'base_head', 'head',
            'current_job_id', 'status', 'fix_count', 'stale_count', 'metadata',
        }
        return self._update_entity(
            'attempts', attempt_id, changes, allowed, {'metadata'},
            'attempt.updated')

    def bump_attempt(self, attempt_id, fixes=0, stale_count=0, **changes):
        allowed = {
            'direction_id', 'branch', 'worktree', 'base_head', 'head',
            'current_job_id', 'status', 'metadata',
        }
        values = self._changed(changes, allowed, {'metadata'})
        now = _timestamp()
        assignments = [
            'fix_count = fix_count + ?',
            'stale_count = stale_count + ?',
        ]
        assignments.extend('{} = ?'.format(key) for key in values)
        with self._transaction() as db:
            attempt = db.execute(
                'SELECT campaign_id FROM attempts WHERE id = ?', (attempt_id,)
            ).fetchone()
            if attempt is None:
                raise KeyError(attempt_id)
            db.execute(
                'UPDATE attempts SET {}, updated_at = ? WHERE id = ?'.format(
                    ', '.join(assignments)),
                [fixes, stale_count] + list(values.values()) +
                [now, attempt_id],
            )
            self._audit(db, attempt['campaign_id'], 'attempt.updated', {
                'attempt_id': attempt_id,
                'fixes_delta': fixes,
                'stale_count_delta': stale_count,
            }, now)
            return _row(db.execute(
                'SELECT * FROM attempts WHERE id = ?', (attempt_id,)
            ).fetchone())

    def add_job(
        self, campaign_id, job_id, role, backend_job_id=None, attempt_id=None,
        direction_id=None, status='queued', metadata=None,
        terminal_payload=None, now=None,
    ):
        now = _timestamp(now)
        backend_job_id = str(job_id if backend_job_id is None else backend_job_id)
        with self._transaction() as db:
            db.execute(
                'INSERT INTO jobs '
                '(id, campaign_id, attempt_id, direction_id, backend_job_id, '
                'role, status, metadata, created_at, updated_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (str(job_id), campaign_id, attempt_id, direction_id,
                 backend_job_id, role, status, _dump(metadata or {}), now, now),
            )
            if attempt_id is not None:
                db.execute(
                    'UPDATE attempts SET current_job_id = ?, updated_at = ? '
                    'WHERE id = ?', (str(job_id), now, attempt_id))
            if terminal_payload is not None:
                db.execute(
                    'UPDATE jobs SET terminal_at = ? WHERE id = ?',
                    (now, str(job_id)))
                cursor = db.execute(
                    'INSERT INTO terminal_events '
                    '(campaign_id, job_id, kind, payload, created_at) '
                    'VALUES (?, ?, ?, ?, ?)',
                    (campaign_id, str(job_id), 'terminal',
                     _dump(terminal_payload), now))
                event_payload = {
                    'event_id': cursor.lastrowid, 'job_id': str(job_id)}
                self._outbox(
                    db, campaign_id, 'explore.terminal', event_payload,
                    'terminal:{}:terminal'.format(job_id), now)
                self._audit(db, campaign_id, 'job.terminal', event_payload, now)
            self._audit(db, campaign_id, 'job.added',
                        {'job_id': str(job_id), 'role': role}, now)
            return _row(db.execute(
                'SELECT * FROM jobs WHERE id = ?', (str(job_id),)
            ).fetchone())

    def get_job(self, job_id):
        return self._fetchone('SELECT * FROM jobs WHERE id = ?', (str(job_id),))

    def list_jobs(
        self, campaign_id=None, attempt_id=None, role=None, status=None,
        uninspected=False,
    ):
        clauses, params = [], []
        for field, value in (
            ('campaign_id', campaign_id), ('attempt_id', attempt_id),
            ('role', role), ('status', status),
        ):
            if value is not None:
                clauses.append('{} = ?'.format(field))
                params.append(value)
        if uninspected:
            clauses.append('inspected_at IS NULL')
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        return self._fetchall(
            'SELECT * FROM jobs{} ORDER BY created_at, id'.format(where), params)

    def update_job(self, job_id, **changes):
        allowed = {
            'attempt_id', 'direction_id', 'backend_job_id', 'role', 'status',
            'terminal_at', 'inspected_at', 'metadata',
        }
        return self._update_entity(
            'jobs', str(job_id), changes, allowed, {'metadata'}, 'job.updated')

    def add_terminal_event(
        self, job_id, status, payload=None, kind='terminal', now=None,
    ):
        """Record one terminal event per job/kind and update the job atomically."""
        now = _timestamp(now)
        job_id = str(job_id)
        with self._transaction() as db:
            job = db.execute(
                'SELECT campaign_id FROM jobs WHERE id = ?', (job_id,)
            ).fetchone()
            if job is None:
                raise KeyError(job_id)
            campaign_id = job['campaign_id']
            cursor = db.execute(
                'INSERT INTO terminal_events '
                '(campaign_id, job_id, kind, payload, created_at) '
                'VALUES (?, ?, ?, ?, ?) '
                'ON CONFLICT(job_id, kind) DO NOTHING',
                (campaign_id, job_id, kind, _dump(payload or {}), now),
            )
            event = db.execute(
                'SELECT * FROM terminal_events WHERE job_id = ? AND kind = ?',
                (job_id, kind),
            ).fetchone()
            if cursor.rowcount:
                db.execute(
                    'UPDATE jobs SET status = ?, terminal_at = ?, '
                    'updated_at = ? WHERE id = ?',
                    (status, now, now, job_id),
                )
                event_payload = {'event_id': event['id'], 'job_id': job_id}
                self._outbox(
                    db, campaign_id, 'explore.terminal', event_payload,
                    'terminal:{}:{}'.format(job_id, kind), now,
                )
                self._audit(db, campaign_id, 'job.terminal', event_payload, now)
            return _row(event)

    def get_event(self, event_id):
        return self._fetchone(
            'SELECT * FROM terminal_events WHERE id = ?', (event_id,))

    def list_events(self, campaign_id=None, status=None, job_id=None):
        clauses, params = [], []
        for field, value in (
            ('campaign_id', campaign_id), ('status', status), ('job_id', job_id),
        ):
            if value is not None:
                clauses.append('{} = ?'.format(field))
                params.append(str(value) if field == 'job_id' else value)
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        return self._fetchall(
            'SELECT * FROM terminal_events{} ORDER BY id'.format(where), params)

    def claim_event(
        self, worker='controller', campaign_id=None, lease_seconds=60, now=None,
    ):
        now = _timestamp(now)
        clauses = [
            "(status = 'pending' OR "
            "(status = 'processing' AND claim_expires_at <= ?))"
        ]
        params = [now]
        if campaign_id is not None:
            clauses.append('campaign_id = ?')
            params.append(campaign_id)
        with self._transaction() as db:
            row = db.execute(
                'SELECT id FROM terminal_events WHERE {} ORDER BY id LIMIT 1'
                .format(' AND '.join(clauses)), params,
            ).fetchone()
            if row is None:
                return None
            db.execute(
                "UPDATE terminal_events SET status = 'processing', "
                'claimed_by = ?, claimed_at = ?, claim_expires_at = ?, '
                'error = NULL WHERE id = ?',
                (worker, now, _expiry(lease_seconds, now), row['id']),
            )
            return _row(db.execute(
                'SELECT * FROM terminal_events WHERE id = ?', (row['id'],)
            ).fetchone())

    def release_event(self, event_id, error=None):
        return self.update_event(
            event_id, status='pending', claimed_by=None, claimed_at=None,
            claim_expires_at=None, error=error)

    def complete_event(self, event_id, status='completed', error=None, now=None):
        if status not in {'completed', 'failed'}:
            raise ValueError('event completion status must be completed or failed')
        now = _timestamp(now)
        with self._transaction() as db:
            event = db.execute(
                'SELECT campaign_id, job_id FROM terminal_events WHERE id = ?',
                (event_id,),
            ).fetchone()
            if event is None:
                raise KeyError(event_id)
            db.execute(
                'UPDATE terminal_events SET status = ?, completed_at = ?, '
                'claim_expires_at = NULL, error = ? WHERE id = ?',
                (status, now, error, event_id),
            )
            db.execute(
                'UPDATE jobs SET inspected_at = ?, updated_at = ? WHERE id = ?',
                (now, now, event['job_id']),
            )
            self._audit(
                db, event['campaign_id'], 'event.completed',
                {'event_id': event_id, 'status': status}, now,
            )
            return _row(db.execute(
                'SELECT * FROM terminal_events WHERE id = ?', (event_id,)
            ).fetchone())

    def update_event(self, event_id, **changes):
        allowed = {
            'payload', 'status', 'claimed_by', 'claimed_at', 'claim_expires_at',
            'completed_at', 'error',
        }
        return self._update_entity(
            'terminal_events', event_id, changes, allowed, {'payload'},
            'event.updated', updated_at=False)

    def record_mutation_event(
        self, event_id, attempt_id, head, stale_count, artifacts, now=None,
        status='fixing', fix_count=None,
    ):
        """Atomically persist a mutation snapshot and its replay evidence."""
        now = _timestamp(now)
        with self._transaction() as db:
            event = db.execute(
                'SELECT campaign_id, payload FROM terminal_events WHERE id = ?',
                (event_id,),
            ).fetchone()
            if event is None:
                raise KeyError(event_id)
            payload = json.loads(event['payload'])
            payload['mutation_artifacts'] = artifacts
            db.execute(
                'UPDATE terminal_events SET payload = ? WHERE id = ?',
                (_dump(payload), event_id))
            assignments = ['head = ?', 'status = ?', 'stale_count = ?']
            values = [head, status, stale_count]
            if fix_count is not None:
                assignments.append('fix_count = ?')
                values.append(int(fix_count))
            db.execute(
                'UPDATE attempts SET {}, updated_at = ? WHERE id = ?'.format(
                    ', '.join(assignments)),
                values + [now, attempt_id])
            self._audit(
                db, event['campaign_id'], 'mutation.recorded',
                {'event_id': event_id, 'attempt_id': attempt_id, 'head': head}, now)
            return _row(db.execute(
                'SELECT * FROM attempts WHERE id = ?', (attempt_id,)
            ).fetchone())

    def add_decision(
        self, campaign_id, decision, attempt_id=None, event_id=None,
        merge_request_id=None, phase='fix', generation=0, reason='',
        evidence=None, memory_updates=None, next_direction=None, metadata=None,
        dedupe_key=None, now=None,
    ):
        now = _timestamp(now)
        if dedupe_key is None and event_id is not None:
            dedupe_key = 'event:{}'.format(event_id)
        with self._transaction() as db:
            cursor = db.execute(
                'INSERT INTO decisions '
                '(campaign_id, attempt_id, event_id, merge_request_id, phase, '
                'generation, decision, reason, evidence, memory_updates, '
                'next_direction, metadata, dedupe_key, created_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(dedupe_key) DO NOTHING',
                (campaign_id, attempt_id, event_id, merge_request_id, phase,
                 generation, decision, reason, _dump(evidence or []),
                 _dump(memory_updates or []),
                 _dump(next_direction) if next_direction is not None else None,
                 _dump(metadata or {}), dedupe_key, now),
            )
            if cursor.rowcount:
                decision_id = cursor.lastrowid
                self._audit(
                    db, campaign_id, 'fix.decision',
                    {'decision_id': decision_id, 'decision': decision}, now,
                )
            elif dedupe_key is not None:
                decision_id = db.execute(
                    'SELECT id FROM decisions WHERE dedupe_key = ?',
                    (dedupe_key,),
                ).fetchone()['id']
            else:
                raise sqlite3.IntegrityError('duplicate decision')
            return _row(db.execute(
                'SELECT * FROM decisions WHERE id = ?', (decision_id,)
            ).fetchone())

    def list_decisions(
        self, campaign_id=None, attempt_id=None, phase=None, decision=None,
    ):
        clauses, params = [], []
        for field, value in (
            ('campaign_id', campaign_id), ('attempt_id', attempt_id),
            ('phase', phase), ('decision', decision),
        ):
            if value is not None:
                clauses.append('{} = ?'.format(field))
                params.append(value)
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        return self._fetchall(
            'SELECT * FROM decisions{} ORDER BY id'.format(where),
            params,
        )

    def get_decision(self, decision_id):
        return self._fetchone(
            'SELECT * FROM decisions WHERE id = ?', (decision_id,))

    def update_decision(self, decision_id, **changes):
        allowed = {
            'attempt_id', 'event_id', 'merge_request_id', 'phase', 'generation',
            'decision', 'reason', 'evidence', 'memory_updates',
            'next_direction', 'metadata', 'dedupe_key',
        }
        return self._update_entity(
            'decisions', decision_id, changes, allowed,
            {'evidence', 'memory_updates', 'next_direction', 'metadata'},
            'decision.updated', updated_at=False)

    def enqueue_merge_request(
        self, campaign_id, attempt_id, head, accepted_at=None, metadata=None,
        dedupe_key=None,
    ):
        """Atomically mark an exact attempt head accepted and enqueue it."""
        accepted_at = _timestamp(accepted_at)
        dedupe_key = dedupe_key or '{}:{}'.format(attempt_id, head)
        with self._transaction() as db:
            existing = db.execute(
                'SELECT * FROM merge_requests WHERE campaign_id = ? AND '
                'dedupe_key = ?', (campaign_id, dedupe_key),
            ).fetchone()
            if existing is not None:
                return _row(existing)
            sequence = db.execute(
                'SELECT COALESCE(MAX(accepted_seq), 0) + 1 FROM '
                'merge_requests WHERE campaign_id = ?', (campaign_id,),
            ).fetchone()[0]
            cursor = db.execute(
                'INSERT INTO merge_requests '
                '(campaign_id, attempt_id, head, accepted_seq, accepted_at, '
                'metadata, dedupe_key) VALUES (?, ?, ?, ?, ?, ?, ?)',
                (campaign_id, attempt_id, head, sequence, accepted_at,
                 _dump(metadata or {}), dedupe_key),
            )
            request_id = cursor.lastrowid
            updated = db.execute(
                "UPDATE attempts SET status = 'merge_queued', updated_at = ? "
                "WHERE id = ? AND campaign_id = ? AND head = ? AND "
                "status IN ('active', 'fixing')",
                (accepted_at, attempt_id, campaign_id, head),
            )
            if updated.rowcount != 1:
                raise ValueError(
                    'attempt is not eligible for merge queueing: {}'.format(
                        attempt_id))
            payload = {
                'merge_request_id': request_id, 'attempt_id': attempt_id,
                'accepted_seq': sequence,
            }
            self._outbox(
                db, campaign_id, 'explore.merge_requested', payload,
                'merge:{}:{}'.format(campaign_id, dedupe_key), accepted_at,
            )
            self._audit(db, campaign_id, 'merge.enqueued', payload, accepted_at)
            return _row(db.execute(
                'SELECT * FROM merge_requests WHERE id = ?', (request_id,)
            ).fetchone())

    def get_merge_request(self, request_id):
        return self._fetchone(
            'SELECT * FROM merge_requests WHERE id = ?', (request_id,))

    def list_merge_requests(self, campaign_id=None, status=None, active=False):
        clauses, params = [], []
        if campaign_id is not None:
            clauses.append('campaign_id = ?')
            params.append(campaign_id)
        if status is not None:
            clauses.append('status = ?')
            params.append(status)
        if active:
            clauses.append("status IN ('queued', 'processing')")
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        return self._fetchall(
            'SELECT * FROM merge_requests{} ORDER BY accepted_seq, id'.format(
                where), params)

    def claim_merge_request(
        self, campaign_id, worker='controller', lease_seconds=300, now=None,
    ):
        now = _timestamp(now)
        with self._transaction() as db:
            active = db.execute(
                "SELECT 1 FROM merge_requests WHERE campaign_id = ? AND "
                "status = 'processing' AND claim_expires_at > ? LIMIT 1",
                (campaign_id, now),
            ).fetchone()
            if active is not None:
                return None
            row = db.execute(
                "SELECT id FROM merge_requests WHERE campaign_id = ? AND "
                "(status = 'queued' OR (status = 'processing' AND "
                'claim_expires_at <= ?)) ORDER BY accepted_seq LIMIT 1',
                (campaign_id, now),
            ).fetchone()
            if row is None:
                return None
            db.execute(
                "UPDATE merge_requests SET status = 'processing', "
                'claimed_by = ?, claimed_at = ?, claim_expires_at = ? '
                'WHERE id = ?',
                (worker, now, _expiry(lease_seconds, now), row['id']),
            )
            return _row(db.execute(
                'SELECT * FROM merge_requests WHERE id = ?', (row['id'],)
            ).fetchone())

    def record_merge_head(
        self, request_id, attempt_id, expected_head, head, metadata, now=None,
    ):
        """Atomically advance the exact attempt and merge-request head."""
        metadata = dict(metadata or {})
        stage = metadata.get('stage')
        if stage not in _MERGE_HEAD_PREDECESSORS:
            raise ValueError('invalid merge head stage: {}'.format(stage))
        now = _timestamp(now)
        with self._transaction() as db:
            request = db.execute(
                'SELECT * FROM merge_requests WHERE id = ?', (request_id,)
            ).fetchone()
            if request is None:
                raise KeyError(request_id)
            if request['status'] != 'processing':
                raise ValueError(
                    'merge request is not processing: {}'.format(request_id))
            if request['attempt_id'] != attempt_id:
                raise ValueError(
                    'merge request does not belong to attempt: {}'.format(
                        attempt_id))
            attempt = db.execute(
                'SELECT campaign_id, head, status FROM attempts WHERE id = ?',
                (attempt_id,),
            ).fetchone()
            if attempt is None:
                raise KeyError(attempt_id)
            if (attempt['campaign_id'] != request['campaign_id'] or
                    attempt['status'] != 'merge_queued'):
                raise ValueError(
                    'attempt is not eligible for merge integration: {}'.format(
                        attempt_id))

            current_metadata = json.loads(request['metadata'])
            current_stage = current_metadata.get('stage')
            heads_match = request['head'] == head and attempt['head'] == head
            if heads_match and current_stage in _MERGE_HEAD_DOWNSTREAM[stage]:
                return _row(request)
            if current_stage not in _MERGE_HEAD_PREDECESSORS[stage]:
                raise ValueError(
                    'merge request cannot advance from stage {} to {}: {}'.format(
                        current_stage, stage, request_id))
            if (request['head'] != expected_head or
                    attempt['head'] != expected_head):
                raise ValueError(
                    'merge source head changed for request: {}'.format(
                        request_id))

            db.execute(
                'UPDATE attempts SET head = ?, updated_at = ? WHERE id = ?',
                (head, now, attempt_id))
            db.execute(
                'UPDATE merge_requests SET head = ?, metadata = ? WHERE id = ?',
                (head, _dump(metadata), request_id))
            self._audit(db, request['campaign_id'], 'merge.head_recorded', {
                'merge_request_id': request_id,
                'attempt_id': attempt_id,
                'previous_head': expected_head,
                'head': head,
                'stage': stage,
            }, now)
            return _row(db.execute(
                'SELECT * FROM merge_requests WHERE id = ?', (request_id,)
            ).fetchone())

    def abandon_merge_request(
        self, request_id, status, result=None, now=None,
    ):
        """Atomically terminate a merge and abandon its attempt and direction."""
        if status not in {'rejected', 'failed', 'cancelled'}:
            raise ValueError(
                'invalid abandoned merge status: {}'.format(status))
        now = _timestamp(now)
        with self._transaction() as db:
            request = db.execute(
                'SELECT * FROM merge_requests WHERE id = ?', (request_id,)
            ).fetchone()
            if request is None:
                raise KeyError(request_id)
            attempt = db.execute(
                'SELECT campaign_id, direction_id, status FROM attempts '
                'WHERE id = ?', (request['attempt_id'],),
            ).fetchone()
            if attempt is None:
                raise KeyError(request['attempt_id'])
            direction = db.execute(
                'SELECT campaign_id, status FROM directions WHERE id = ?',
                (attempt['direction_id'],),
            ).fetchone()
            if direction is None:
                raise KeyError(attempt['direction_id'])
            if (attempt['campaign_id'] != request['campaign_id'] or
                    direction['campaign_id'] != request['campaign_id']):
                raise ValueError(
                    'merge request ownership is inconsistent: {}'.format(
                        request_id))

            already_abandoned = (
                request['status'] == status and
                attempt['status'] == 'abandoned' and
                direction['status'] == 'abandoned')
            if already_abandoned:
                return _row(request)
            if request['status'] not in {'queued', 'processing', status}:
                raise ValueError(
                    'merge request cannot be abandoned from status {}: {}'
                    .format(request['status'], request_id))
            if attempt['status'] not in {'merge_queued', 'abandoned'}:
                raise ValueError(
                    'merge attempt cannot be abandoned from status {}: {}'
                    .format(attempt['status'], request['attempt_id']))
            if direction['status'] in {'accepted'}:
                raise ValueError(
                    'accepted direction cannot be abandoned: {}'.format(
                        attempt['direction_id']))

            db.execute(
                "UPDATE attempts SET status = 'abandoned', updated_at = ? "
                'WHERE id = ?', (now, request['attempt_id']))
            db.execute(
                "UPDATE directions SET status = 'abandoned', updated_at = ? "
                'WHERE id = ?', (now, attempt['direction_id']))
            db.execute(
                'UPDATE merge_requests SET status = ?, result = ?, '
                'completed_at = ?, claimed_by = NULL, claimed_at = NULL, '
                'claim_expires_at = NULL WHERE id = ?',
                (status, _dump(result) if result is not None else None,
                 now, request_id),
            )
            self._audit(
                db, request['campaign_id'], 'merge.completed',
                {'merge_request_id': request_id, 'status': status}, now,
            )
            return _row(db.execute(
                'SELECT * FROM merge_requests WHERE id = ?', (request_id,)
            ).fetchone())

    def finalize_merge_request(
        self, request_id, head, campaign_config=None, result=None, now=None,
    ):
        """Atomically advance campaign state after Git fast-forwarded."""
        now = _timestamp(now)
        with self._transaction() as db:
            request = db.execute(
                'SELECT * FROM merge_requests WHERE id = ?', (request_id,)
            ).fetchone()
            if request is None:
                raise KeyError(request_id)
            attempt = db.execute(
                'SELECT campaign_id, direction_id, head, status FROM attempts '
                'WHERE id = ?',
                (request['attempt_id'],),
            ).fetchone()
            if attempt is None:
                raise KeyError(request['attempt_id'])
            direction = db.execute(
                'SELECT campaign_id, status FROM directions WHERE id = ?',
                (attempt['direction_id'],),
            ).fetchone()
            if direction is None:
                raise KeyError(attempt['direction_id'])
            campaign = db.execute(
                'SELECT generation, mainline_head, config FROM campaigns '
                'WHERE id = ?',
                (request['campaign_id'],),
            ).fetchone()
            if campaign is None:
                raise KeyError(request['campaign_id'])
            if (attempt['campaign_id'] != request['campaign_id'] or
                    direction['campaign_id'] != request['campaign_id']):
                raise ValueError(
                    'merge request ownership is inconsistent: {}'.format(
                        request_id))
            if head != request['head']:
                raise ValueError(
                    'merged head does not match merge request: {}'.format(
                        request_id))
            if request['status'] == 'merged':
                if (attempt['head'] != head or
                        attempt['status'] != 'merged' or
                        direction['status'] != 'accepted' or
                        campaign['mainline_head'] != head):
                    raise ValueError(
                        'merged request state is inconsistent: {}'.format(
                            request_id))
                return _row(request)
            if request['status'] != 'processing':
                raise ValueError(
                    'merge request is not processing: {}'.format(request_id))
            if attempt['head'] != head or attempt['status'] != 'merge_queued':
                raise ValueError(
                    'attempt is not eligible for merge finalization: {}'.format(
                        request['attempt_id']))
            config = (
                _dump(campaign_config) if campaign_config is not None
                else campaign['config'])
            db.execute(
                'UPDATE campaigns SET mainline_head = ?, generation = ?, '
                'stall_count = 0, config = ?, updated_at = ? WHERE id = ?',
                (head, campaign['generation'] + 1, config, now,
                 request['campaign_id']),
            )
            db.execute(
                "UPDATE attempts SET head = ?, status = 'merged', updated_at = ? "
                'WHERE id = ?', (head, now, request['attempt_id']))
            db.execute(
                "UPDATE directions SET status = 'accepted', updated_at = ? "
                'WHERE id = ?', (now, attempt['direction_id']))
            db.execute(
                "UPDATE merge_requests SET status = 'merged', result = ?, "
                'completed_at = ?, claimed_by = NULL, claimed_at = NULL, '
                'claim_expires_at = NULL WHERE id = ?',
                (_dump(result or {'head': head}), now, request_id),
            )
            self._audit(
                db, request['campaign_id'], 'merge.completed',
                {'merge_request_id': request_id, 'status': 'merged'}, now)
            return _row(db.execute(
                'SELECT * FROM merge_requests WHERE id = ?', (request_id,)
            ).fetchone())

    def update_merge_request(self, request_id, **changes):
        allowed = {
            'head', 'status', 'claimed_by', 'claimed_at', 'claim_expires_at',
            'completed_at', 'result', 'metadata',
        }
        return self._update_entity(
            'merge_requests', request_id, changes, allowed,
            {'result', 'metadata'}, 'merge.updated', updated_at=False)

    def merge_queue_empty(self, campaign_id):
        row = self._fetchone(
            "SELECT COUNT(*) AS n FROM merge_requests WHERE campaign_id = ? "
            "AND status IN ('queued', 'processing')", (campaign_id,))
        return row['n'] == 0

    def add_finding(
        self, claim, trust, campaign_id=None, attempt_id=None,
        direction_id=None, outcome=None, confidence=None, scope=None,
        source_commit=None, provenance=None, metadata=None, dedupe_key=None,
        now=None,
    ):
        now = _timestamp(now)
        with self._transaction() as db:
            cursor = db.execute(
                'INSERT INTO findings '
                '(campaign_id, attempt_id, direction_id, claim, outcome, trust, '
                'confidence, scope, source_commit, provenance, metadata, '
                'dedupe_key, created_at, updated_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(dedupe_key) DO NOTHING',
                (campaign_id, attempt_id, direction_id, claim, outcome, trust,
                 confidence, scope, source_commit, _dump(provenance or {}),
                 _dump(metadata or {}), dedupe_key, now, now),
            )
            if cursor.rowcount:
                finding_id = cursor.lastrowid
                self._audit(
                    db, campaign_id, 'finding.added',
                    {'finding_id': finding_id, 'trust': trust}, now,
                )
            elif dedupe_key is not None:
                finding_id = db.execute(
                    'SELECT id FROM findings WHERE dedupe_key = ?',
                    (dedupe_key,),
                ).fetchone()['id']
            else:
                raise sqlite3.IntegrityError('duplicate finding')
            return _row(db.execute(
                'SELECT * FROM findings WHERE id = ?', (finding_id,)
            ).fetchone())

    def get_finding(self, finding_id):
        return self._fetchone(
            'SELECT * FROM findings WHERE id = ?', (finding_id,))

    def list_findings(
        self, campaign_id=None, trust=None, outcome=None, limit=None,
    ):
        clauses, params = [], []
        for field, value in (
            ('campaign_id', campaign_id), ('trust', trust), ('outcome', outcome),
        ):
            if value is not None:
                clauses.append('{} = ?'.format(field))
                params.append(value)
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        query = 'SELECT * FROM findings{} ORDER BY created_at DESC, id DESC'.format(
            where)
        if limit is not None:
            query += ' LIMIT ?'
            params.append(limit)
        return self._fetchall(query, params)

    def update_finding(self, finding_id, **changes):
        allowed = {
            'claim', 'outcome', 'trust', 'confidence', 'scope', 'source_commit',
            'provenance', 'metadata', 'dedupe_key',
        }
        return self._update_entity(
            'findings', finding_id, changes, allowed,
            {'provenance', 'metadata'}, 'finding.updated')

    def emit(
        self, campaign_id, kind, payload=None, outbox_topic=None,
        dedupe_key=None, now=None,
    ):
        """Atomically append an audit record and, optionally, an outbox item."""
        now = _timestamp(now)
        payload = payload or {}
        with self._transaction() as db:
            audit_id = self._audit(db, campaign_id, kind, payload, now)
            outbox_id = None
            if outbox_topic is not None:
                if dedupe_key is None:
                    dedupe_key = '{}:{}:{}'.format(
                        campaign_id or 'project', kind, audit_id)
                outbox_id = self._outbox(
                    db, campaign_id, outbox_topic, payload, dedupe_key, now)
            return {
                'audit': _row(db.execute(
                    'SELECT * FROM audit_events WHERE id = ?', (audit_id,)
                ).fetchone()),
                'outbox': _row(db.execute(
                    'SELECT * FROM outbox WHERE id = ?', (outbox_id,)
                ).fetchone()) if outbox_id is not None else None,
            }

    def list_audit(self, campaign_id=None, kind=None, after_id=None):
        clauses, params = [], []
        for field, value in (
            ('campaign_id', campaign_id), ('kind', kind),
        ):
            if value is not None:
                clauses.append('{} = ?'.format(field))
                params.append(value)
        if after_id is not None:
            clauses.append('id > ?')
            params.append(after_id)
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        return self._fetchall(
            'SELECT * FROM audit_events{} ORDER BY id'.format(where), params)

    def list_outbox(self, campaign_id=None, status=None, topic=None):
        clauses, params = [], []
        for field, value in (
            ('campaign_id', campaign_id), ('status', status), ('topic', topic),
        ):
            if value is not None:
                clauses.append('{} = ?'.format(field))
                params.append(value)
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        return self._fetchall(
            'SELECT * FROM outbox{} ORDER BY id'.format(where), params)

    def claim_outbox(
        self, worker='controller', campaign_id=None, lease_seconds=60, now=None,
    ):
        now = _timestamp(now)
        clauses = [
            "(status = 'pending' OR "
            "(status = 'processing' AND claim_expires_at <= ?))"
        ]
        params = [now]
        if campaign_id is not None:
            clauses.append('campaign_id = ?')
            params.append(campaign_id)
        with self._transaction() as db:
            row = db.execute(
                'SELECT id FROM outbox WHERE {} ORDER BY id LIMIT 1'.format(
                    ' AND '.join(clauses)), params,
            ).fetchone()
            if row is None:
                return None
            db.execute(
                "UPDATE outbox SET status = 'processing', claimed_by = ?, "
                'claimed_at = ?, claim_expires_at = ?, error = NULL '
                'WHERE id = ?',
                (worker, now, _expiry(lease_seconds, now), row['id']),
            )
            return _row(db.execute(
                'SELECT * FROM outbox WHERE id = ?', (row['id'],)
            ).fetchone())

    def release_outbox(self, outbox_id, error=None):
        return self.update_outbox(
            outbox_id, status='pending', claimed_by=None, claimed_at=None,
            claim_expires_at=None, error=error)

    def complete_outbox(
        self, outbox_id, status='completed', error=None, now=None,
    ):
        if status not in {'completed', 'failed'}:
            raise ValueError('outbox completion status must be completed or failed')
        return self.update_outbox(
            outbox_id, status=status, completed_at=_timestamp(now),
            claim_expires_at=None, error=error)

    def update_outbox(self, outbox_id, **changes):
        allowed = {
            'payload', 'status', 'claimed_by', 'claimed_at', 'claim_expires_at',
            'completed_at', 'error',
        }
        return self._update_entity(
            'outbox', outbox_id, changes, allowed, {'payload'},
            'outbox.updated', updated_at=False)

    def counts(self, campaign_id):
        """Return totals and per-status counts used for scheduling and budgets."""
        result = {}
        with self._lock:
            for table in (
                'directions', 'attempts', 'jobs', 'terminal_events',
                'decisions', 'merge_requests', 'findings', 'outbox',
            ):
                result[table] = self._db.execute(
                    'SELECT COUNT(*) FROM {} WHERE campaign_id = ?'.format(table),
                    (campaign_id,),
                ).fetchone()[0]
            for table in ('directions', 'attempts', 'jobs', 'terminal_events',
                          'merge_requests', 'outbox'):
                for row in self._db.execute(
                    'SELECT status, COUNT(*) AS n FROM {} WHERE campaign_id = ? '
                    'GROUP BY status'.format(table), (campaign_id,),
                ):
                    result['{}_{}'.format(table, row['status'])] = row['n']
        return result

    def _update_entity(
        self, table, entity_id, changes, allowed, json_fields, audit_kind,
        updated_at=True,
    ):
        values = self._changed(changes, allowed, json_fields)
        if not values:
            return self._fetchone(
                'SELECT * FROM {} WHERE id = ?'.format(table), (entity_id,))
        now = _timestamp()
        if updated_at:
            values['updated_at'] = now
        assignments = ', '.join('{} = ?'.format(key) for key in values)
        with self._transaction() as db:
            campaign = db.execute(
                'SELECT campaign_id FROM {} WHERE id = ?'.format(table),
                (entity_id,),
            ).fetchone()
            if campaign is None:
                raise KeyError(entity_id)
            db.execute(
                'UPDATE {} SET {} WHERE id = ?'.format(table, assignments),
                list(values.values()) + [entity_id],
            )
            self._audit(db, campaign['campaign_id'], audit_kind, {
                '{}_id'.format(table.rstrip('s')): entity_id,
                'changes': changes,
            }, now)
            return _row(db.execute(
                'SELECT * FROM {} WHERE id = ?'.format(table), (entity_id,)
            ).fetchone())

import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _entity_from_row(row):
    return {
        "id": row["id"],
        "kind": row["kind"],
        "status": row["status"],
        "version": int(row["version"]),
        "data": json.loads(row["data"]),
        "created_by": row["created_by"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _fetch_entity(connection, entity_id):
    row = connection.execute(
        "SELECT * FROM entities WHERE id = ?", (entity_id,)
    ).fetchone()
    return _entity_from_row(row) if row else None


def _fetch_entities(connection, kind=None, status=None):
    clauses = []
    params = []
    if kind:
        clauses.append("kind = ?")
        params.append(kind)
    if status:
        clauses.append("status = ?")
        params.append(status)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = connection.execute(
        "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
    ).fetchall()
    return [_entity_from_row(row) for row in rows]


def _insert_entity(connection, entity_id, kind, status, data, actor_id):
    now = utcnow()
    payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
    connection.execute(
        "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
        "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
        (entity_id, kind, status, payload, actor_id, now, now),
    )
    return _fetch_entity(connection, entity_id)


def _update_entity(connection, entity_id, expected_version, status, data):
    now = utcnow()
    payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
    row = connection.execute(
        "SELECT version FROM entities WHERE id = ?", (entity_id,)
    ).fetchone()
    if not row:
        raise NotFoundError("entity not found: " + entity_id)
    current_version = int(row["version"])
    if expected_version is not None and current_version != int(expected_version):
        raise ConflictError(
            "version conflict: expected %s, found %s"
            % (expected_version, current_version)
        )
    connection.execute(
        "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
        "WHERE id = ? AND version = ?",
        (status, payload, now, entity_id, current_version),
    )
    return _fetch_entity(connection, entity_id)


def _append_audit(connection, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
    connection.execute(
        "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            entity_id,
            actor_id,
            actor_role,
            action,
            from_status,
            to_status,
            json.dumps(detail, ensure_ascii=False, sort_keys=True),
            utcnow(),
        ),
    )


def _get_action_idempotency(connection, actor_id, idem_key):
    row = connection.execute(
        "SELECT entity_id, action, to_status, version FROM action_idempotency "
        "WHERE actor_id = ? AND idem_key = ?",
        (actor_id, idem_key),
    ).fetchone()
    if not row:
        return None
    return {
        "entity_id": row["entity_id"],
        "action": row["action"],
        "to_status": row["to_status"],
        "version": int(row["version"]),
    }


def _save_action_idempotency(connection, actor_id, idem_key, entity_id, action, to_status, version):
    connection.execute(
        "INSERT OR REPLACE INTO action_idempotency(actor_id, idem_key, entity_id, action, to_status, version, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (actor_id, idem_key, entity_id, action, to_status, int(version), utcnow()),
    )


class RepositoryTransaction:
    """Connection-bound operations inside SQLiteRepository.run_in_transaction.

    Every write issued here commits or rolls back together with the rest of
    the surrounding unit of work, so multi-entity governance steps stay atomic.
    """

    def __init__(self, connection):
        self.connection = connection

    def get_entity(self, entity_id):
        return _fetch_entity(self.connection, entity_id)

    def list_entities(self, kind=None, status=None):
        return _fetch_entities(self.connection, kind=kind, status=status)

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        return _update_entity(self.connection, entity_id, expected_version, status, data)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        _append_audit(
            self.connection, entity_id, actor_id, actor_role, action, from_status, to_status, detail
        )

    def get_action_idempotency(self, actor_id, idem_key):
        return _get_action_idempotency(self.connection, actor_id, idem_key)

    def save_action_idempotency(self, actor_id, idem_key, entity_id, action, to_status, version):
        _save_action_idempotency(
            self.connection, actor_id, idem_key, entity_id, action, to_status, version
        )


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS action_idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    to_status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
            """)

    def run_in_transaction(self, work):
        """Run work(RepositoryTransaction) under an immediate write lock.

        BEGIN IMMEDIATE serializes writers, so read-modify-write sequences
        (capacity checks, revocation reconciliation) are atomic. Any failure
        rolls everything back and the caller may retry from the last
        committed state.
        """
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            result = work(RepositoryTransaction(connection))
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def create_entity(self, entity_id, kind, status, data, actor_id):
        with self._connect() as connection:
            return _insert_entity(connection, entity_id, kind, status, data, actor_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            return _fetch_entity(connection, entity_id)

    def list_entities(self, kind=None, status=None):
        with self._connect() as connection:
            return _fetch_entities(connection, kind=kind, status=status)

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        def work(tx):
            return tx.update_entity(entity_id, expected_version, status, data)

        return self.run_in_transaction(work)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            _append_audit(
                connection, entity_id, actor_id, actor_role, action, from_status, to_status, detail
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def get_action_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            return _get_action_idempotency(connection, actor_id, idem_key)

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True

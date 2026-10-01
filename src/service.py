import sqlite3
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, InvalidTransition, NotFoundError
from .repository import utcnow
from .rules import DEFAULT_CAPACITY, RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if kind == "dataset":
            payload.setdefault("capacity", DEFAULT_CAPACITY)
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        kind = self.rules.normalize_kind(entity["kind"])
        if kind == "grant" and action == "activate":
            return self._activate_grant(actor, entity_id, data, expected_version)
        if kind == "grant" and action in ("revoke", "expire"):
            target = "revoked" if action == "revoke" else "expired"
            if entity["status"] == target:
                self.rules.ensure_role(
                    actor, self.rules.ROLE_ACTIONS.get(("grant", action), ("admin", "committee"))
                )
                self._reconcile_sessions(entity, actor, action)
                return entity
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected_version, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        if kind == "grant" and action in ("revoke", "expire"):
            self._reconcile_sessions(updated, actor, action)
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    # ------------------------------------------------------------------
    # 容量预约与会话启用
    # ------------------------------------------------------------------

    def _dataset_capacity(self, dataset_id):
        if not dataset_id:
            return DEFAULT_CAPACITY
        dataset = self.repository.get_entity(dataset_id)
        if not dataset:
            return DEFAULT_CAPACITY
        capacity = dataset["data"].get("capacity", DEFAULT_CAPACITY)
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            return DEFAULT_CAPACITY
        return capacity

    def _sessions_on_dataset(self, dataset_id, status=None):
        sessions = self.repository.list_entities(kind="session", status=status)
        return [s for s in sessions if s["data"].get("dataset_id") == dataset_id]

    def _find_session_for_grant(self, grant_id):
        sessions = self.repository.find_entities("session", "grant_id", grant_id)
        return sessions[0] if sessions else None

    def _earliest_release(self, active_sessions):
        ends = [s["data"].get("expires_at") for s in active_sessions if s["data"].get("expires_at")]
        return min(ends) if ends else None

    def _ensure_session(self, grant, actor, activate_data=None):
        existing = self._find_session_for_grant(grant["id"])
        if existing is not None:
            return existing
        activate_data = activate_data or {}
        dataset_id = grant["data"].get("dataset_id")
        expires_at = activate_data.get("expires_at") or grant["data"].get("expires_at")
        capacity = self._dataset_capacity(dataset_id)
        active = self._sessions_on_dataset(dataset_id, "active")
        if len(active) < capacity:
            status = "active"
            release_time = None
        else:
            status = "queued"
            release_time = self._earliest_release(active)
        now = utcnow()
        session_data = {
            "grant_id": grant["id"],
            "dataset_id": dataset_id,
            "recipient": grant["data"].get("recipient"),
            "expires_at": expires_at,
            "queued_at": now,
            "activated_at": now if status == "active" else None,
            "released_at": None,
            "release_time": release_time,
            "stopped_at": None,
            "frozen_at": None,
        }
        try:
            session = self.repository.create_entity(
                str(uuid4()), "session", status, session_data, actor.user_id
            )
        except sqlite3.IntegrityError:
            # 唯一索引保证同一 grant 只会有一个会话；并发激活时复用已创建的会话
            session = self._find_session_for_grant(grant["id"])
            if session is None:
                raise
        self.audit.record(
            session["id"],
            actor,
            "session_created",
            None,
            status,
            {"grant_id": grant["id"], "release_time": release_time},
        )
        return session

    def _activate_grant(self, actor, grant_id, data, expected_version):
        grant = self.repository.get_entity(grant_id)
        if not grant:
            raise NotFoundError("entity not found: " + grant_id)
        data = dict(data or {})
        if grant["status"] != "issued":
            raise InvalidTransition(
                "cannot activate grant from status %s" % grant["status"]
            )
        patch = self.rules.validate_activate(actor, data)
        merged = dict(grant["data"])
        merged.update(data)
        merged.update(patch)
        session = self._ensure_session(grant, actor, data)
        # 以 grant 当前版本作为乐观锁条件：两人同时激活同一 grant 时只放行一个
        expected = expected_version if expected_version is not None else grant["version"]
        updated = self.repository.update_entity(grant_id, expected, "active", merged)
        self.audit.record(
            grant_id,
            actor,
            "activate",
            grant["status"],
            "active",
            {"session_id": session["id"]},
        )
        return self._activate_response(updated, session)

    def _activate_response(self, grant, session):
        result = dict(grant)
        result["session"] = session
        result["queued"] = session["status"] == "queued"
        result["release_time"] = (
            session["data"].get("release_time") if session["status"] == "queued" else None
        )
        return result

    # ------------------------------------------------------------------
    # 撤回/到期对账
    # ------------------------------------------------------------------

    def _find_sessions_for_grant(self, grant_id):
        return self.repository.find_entities("session", "grant_id", grant_id)

    def _freeze_session(self, session, actor):
        if session["status"] != "queued":
            return session
        now = utcnow()
        data = dict(session["data"])
        data["frozen_at"] = now
        updated = self.repository.update_entity(session["id"], session["version"], "frozen", data)
        self.audit.record(
            session["id"],
            actor,
            "freeze",
            "queued",
            "frozen",
            {"grant_id": session["data"].get("grant_id")},
        )
        return updated

    def _stop_session(self, session, actor):
        if session["status"] != "active":
            return session
        now = utcnow()
        data = dict(session["data"])
        data["stopped_at"] = now
        updated = self.repository.update_entity(session["id"], session["version"], "stopped", data)
        self.audit.record(
            session["id"],
            actor,
            "stop",
            "active",
            "stopped",
            {"grant_id": session["data"].get("grant_id")},
        )
        return updated

    def _drain_queue(self, dataset_id, actor):
        if not dataset_id:
            return
        capacity = self._dataset_capacity(dataset_id)
        active = self._sessions_on_dataset(dataset_id, "active")
        queued = self._sessions_on_dataset(dataset_id, "queued")
        queued.sort(key=lambda s: s["data"].get("queued_at") or "")
        for session in queued:
            if len(active) >= capacity:
                break
            now = utcnow()
            data = dict(session["data"])
            data["activated_at"] = now
            data["release_time"] = None
            updated = self.repository.update_entity(session["id"], session["version"], "active", data)
            self.audit.record(
                session["id"],
                actor,
                "session_activated",
                "queued",
                "active",
                {"grant_id": session["data"].get("grant_id")},
            )
            active.append(updated)

    def _reconcile_sessions(self, grant, actor, action):
        for session in self._find_sessions_for_grant(grant["id"]):
            if session["status"] == "queued":
                self._freeze_session(session, actor)
            elif session["status"] == "active":
                self._stop_session(session, actor)
        self._drain_queue(grant["data"].get("dataset_id"), actor)

from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, InvalidTransition, NotFoundError
from .repository import utcnow
from .rules import RuleEngine, dataset_capacity, valid_grant_window

# session statuses reconciled when their grant is revoked or expires
RECONCILE_MAP = {"requested": "frozen", "queued": "frozen", "active": "stopped"}


def _earliest_release(active_sessions, fallback):
    """When the next capacity slot is expected to free up."""
    leases = [
        str(item["data"]["lease_expires_at"])
        for item in active_sessions
        if item["data"].get("lease_expires_at")
    ]
    if leases:
        return min(leases)
    return fallback or None


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
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        payload = self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None, idempotency_key=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        if idempotency_key:
            # replay of an already committed action: return the recorded result
            # instead of re-executing, so retries resume from the last progress
            existing = self.repository.get_action_idempotency(actor.user_id, idempotency_key)
            if existing:
                if existing["entity_id"] != entity_id or existing["action"] != action:
                    raise ConflictError(
                        "idempotency key was already used for a different action"
                    )
                return self.get(entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        if entity["kind"] == "session" and action == "start":
            return self._start_session(actor, entity, expected, patch, merged, idempotency_key)
        if entity["kind"] == "grant" and action in ("revoke", "expire"):
            return self._close_grant(
                actor, entity, action, next_status, expected, patch, merged, idempotency_key
            )
        if entity["kind"] == "session" and action == "record":
            records = list(entity["data"].get("access_records") or [])
            records.append({"at": utcnow(), "actor": actor.user_id, "note": patch.get("note", "")})
            merged["access_records"] = records

        def work(tx):
            updated = tx.update_entity(entity_id, expected, next_status, merged)
            tx.append_audit(
                entity_id,
                actor.user_id,
                actor.role,
                action,
                entity["status"],
                updated["status"],
                {"patch": patch},
            )
            self._remember(tx, actor, idempotency_key, entity_id, action, updated)
            return updated

        return self.repository.run_in_transaction(work)

    def _start_session(self, actor, session, expected, patch, merged, idempotency_key):
        """Reserve a capacity slot, or queue the session and report release time."""
        entity_id = session["id"]
        dataset_id = session["data"].get("dataset_id")

        def work(tx):
            grant = tx.get_entity(session["data"].get("grant_id"))
            if not grant or grant["status"] != "active":
                raise InvalidTransition("grant is not active; session cannot start")
            grant_expires = str(grant["data"].get("expires_at") or "")
            if grant_expires and not valid_grant_window(grant_expires, utcnow()[:10]):
                raise InvalidTransition("grant window has expired; session cannot start")
            capacity = dataset_capacity(tx.get_entity(dataset_id))
            active_sessions = [
                item
                for item in tx.find_entities("session", "dataset_id", dataset_id)
                if item["status"] == "active"
            ]
            now = utcnow()
            if len(active_sessions) < capacity:
                status = "active"
                lease = merged.get("lease_expires_at") or grant_expires
                if grant_expires:
                    lease = min(str(lease), grant_expires)
                merged["reserved_at"] = now
                merged["lease_expires_at"] = lease
                merged.pop("release_at", None)
            else:
                status = "queued"
                merged["queued_at"] = now
                merged["release_at"] = _earliest_release(active_sessions, grant_expires)
            updated = tx.update_entity(entity_id, expected, status, merged)
            tx.append_audit(
                entity_id,
                actor.user_id,
                actor.role,
                "start",
                session["status"],
                status,
                {"patch": patch, "capacity": capacity, "active": len(active_sessions)},
            )
            self._remember(tx, actor, idempotency_key, entity_id, "start", updated)
            return updated

        return self.repository.run_in_transaction(work)

    def _close_grant(self, actor, grant, action, next_status, expected, patch, merged, idempotency_key):
        """Revoke/expire a grant and reconcile its sessions in one transaction.

        Sessions that never started are frozen; sessions already reading data
        are stopped but keep their access records for the audit trail.
        """
        entity_id = grant["id"]
        reason = merged.get("reason") or merged.get("expired_at") or action

        def work(tx):
            updated = tx.update_entity(entity_id, expected, next_status, merged)
            reconciled = []
            for item in tx.find_entities("session", "grant_id", entity_id):
                target = RECONCILE_MAP.get(item["status"])
                if not target:
                    continue
                session_data = dict(item["data"])
                session_data[target + "_at"] = utcnow()
                session_data[target + "_reason"] = "grant %s: %s" % (action, reason)
                tx.update_entity(item["id"], item["version"], target, session_data)
                tx.append_audit(
                    item["id"],
                    actor.user_id,
                    actor.role,
                    "freeze" if target == "frozen" else "stop",
                    item["status"],
                    target,
                    {
                        "grant_id": entity_id,
                        "reason": reason,
                        "access_records": len(session_data.get("access_records") or []),
                    },
                )
                reconciled.append({"id": item["id"], "from": item["status"], "to": target})
            tx.append_audit(
                entity_id,
                actor.user_id,
                actor.role,
                action,
                grant["status"],
                updated["status"],
                {"patch": patch, "reconciled": reconciled},
            )
            self._remember(tx, actor, idempotency_key, entity_id, action, updated)
            result = dict(updated)
            result["reconciled_sessions"] = reconciled
            return result

        return self.repository.run_in_transaction(work)

    @staticmethod
    def _remember(tx, actor, idempotency_key, entity_id, action, updated):
        if idempotency_key:
            tx.save_action_idempotency(
                actor.user_id,
                idempotency_key,
                entity_id,
                action,
                updated["status"],
                updated["version"],
            )

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

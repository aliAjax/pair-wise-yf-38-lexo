from datetime import datetime, timedelta

from .domain import (
    ConflictError,
    InvalidTransition,
    PermissionDenied,
    ValidationError,
)


DEFAULT_DATASET_CAPACITY = 1


def _validate_capacity_value(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValidationError("capacity must be a positive integer")


def dataset_capacity(dataset):
    """Max concurrent active sessions a dataset allows (default 1)."""
    data = (dataset or {}).get("data") or {}
    raw = data.get("capacity", DEFAULT_DATASET_CAPACITY)
    _validate_capacity_value(raw)
    return raw


def _validate_dataset(actor, data, lookup):
    if len(data.get("access_policy", "")) < 3:
        raise ValidationError("access_policy is required")
    if "capacity" in data:
        _validate_capacity_value(data["capacity"])


def _validate_set_capacity(actor, entity, data, lookup):
    _validate_capacity_value(data.get("capacity"))


def _validate_session(actor, data, lookup):
    grant = _find_one(lookup, "grant", "id", data.get("grant_id"))
    if not grant:
        raise ValidationError("grant does not exist")
    if grant["status"] != "active":
        raise ValidationError("grant is not active")
    extra = {
        "dataset_id": grant["data"].get("dataset_id"),
        "recipient": grant["data"].get("recipient"),
    }
    if grant["data"].get("expires_at"):
        extra["grant_expires_at"] = grant["data"].get("expires_at")
    return extra


def _validate_application(actor, data, lookup):
    dataset = _find_one(lookup, "dataset", "id", data.get("dataset_id"))
    if not dataset:
        raise ValidationError("dataset does not exist")
    if not data.get("purpose", "").strip():
        raise ValidationError("purpose is required")


def _validate_approve(actor, entity, data, lookup):
    approvals = data.get("approvals") or []
    if len(set(approvals)) < 3:
        raise ValidationError("at least three distinct committee approvals are required")
    if data.get("conflict_of_interest"):
        raise PermissionDenied("conflicted reviewer cannot approve access")


def valid_grant_window(expires_at, as_of):
    return str(expires_at) >= str(as_of)


def _validate_grant_activate(actor, entity, data, lookup):
    if data.get("expires_at") < data.get("starts_at"):
        raise ValidationError("grant expiry must be after start")
    return {"activated_by": actor.user_id}


CUSTOM_CREATE = {'dataset': _validate_dataset, 'application': _validate_application, 'session': _validate_session}
CUSTOM_TRANSITIONS = {('application', 'approve'): _validate_approve, ('grant', 'activate'): _validate_grant_activate, ('dataset', 'set_capacity'): _validate_set_capacity}


class RuleEngine:
    ALIASES = {'datasets': 'dataset', 'applications': 'application', 'grants': 'grant', 'sessions': 'session'}
    INITIAL_STATUS = {'dataset': 'registered', 'application': 'draft', 'grant': 'issued', 'session': 'requested'}
    # action: (allowed from-statuses, next status); next status None keeps the current one
    TRANSITIONS = {
        'dataset': {
            'restrict': (('registered',), 'restricted'),
            'publish': (('restricted',), 'published'),
            'set_capacity': (('registered', 'restricted', 'published'), None),
        },
        'application': {
            'submit': (('draft',), 'submitted'),
            'review': (('submitted',), 'under_review'),
            'approve': (('under_review',), 'approved'),
            'reject': (('under_review',), 'rejected'),
            'withdraw': (('submitted', 'under_review'), 'withdrawn'),
        },
        'grant': {
            'activate': (('issued',), 'active'),
            'revoke': (('active',), 'revoked'),
            'expire': (('active',), 'expired'),
        },
        'session': {
            'start': (('requested', 'queued'), 'active'),
            'release': (('active',), 'completed'),
            'record': (('active',), None),
        },
    }
    CREATE_REQUIRED = {'dataset': ('name', 'access_policy'), 'application': ('dataset_id', 'applicant_id', 'purpose'), 'grant': ('application_id', 'dataset_id', 'recipient'), 'session': ('grant_id',)}
    ACTION_REQUIRED = {('dataset', 'restrict'): ('reason',), ('dataset', 'set_capacity'): ('capacity',), ('application', 'review'): ('committee_id',), ('application', 'approve'): ('approvals', 'terms', 'expires_at'), ('application', 'reject'): ('reason',), ('application', 'withdraw'): ('reason',), ('grant', 'activate'): ('starts_at', 'expires_at'), ('grant', 'revoke'): ('reason',), ('grant', 'expire'): ('expired_at',), ('session', 'record'): ('note',)}
    CREATE_ROLES = {'dataset': ('admin', 'committee'), 'application': ('admin', 'applicant'), 'grant': ('admin', 'committee'), 'session': ('admin', 'applicant')}
    ROLE_ACTIONS = {'restrict': ('admin', 'committee'), 'publish': ('admin', 'committee'), 'set_capacity': ('admin', 'committee'), 'submit': ('admin', 'applicant'), 'review': ('admin', 'committee'), 'approve': ('admin', 'committee'), 'reject': ('admin', 'committee'), 'withdraw': ('admin', 'applicant'), 'activate': ('admin', 'committee'), 'revoke': ('admin', 'committee'), 'expire': ('admin', 'committee'), 'start': ('admin', 'applicant'), 'release': ('admin', 'applicant'), 'record': ('admin', 'applicant')}

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

    @staticmethod
    def _ensure_role(actor, allowed):
        if "*" not in allowed and actor.role not in allowed:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

    @staticmethod
    def _require(data, fields):
        for field in fields:
            value = data.get(field)
            if value is None or value == "" or value == [] or value == {}:
                raise ValidationError("missing required field: " + field)

    def validate_create(self, actor, kind, data, lookup=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        self._ensure_role(actor, self.CREATE_ROLES.get(kind, ("admin",)))
        self._require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = CUSTOM_CREATE.get(kind)
        extra = custom(actor, data, lookup) if custom else None
        payload = dict(data)
        if extra:
            payload.update(extra)
        return payload

    def validate_transition(self, actor, entity, action, data, lookup=None):
        kind = self.normalize_kind(entity["kind"])
        transition = self.TRANSITIONS.get(kind, {}).get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition(
                "cannot %s from status %s" % (action, entity["status"])
            )
        if next_status is None:
            next_status = entity["status"]
        allowed_roles = self.ROLE_ACTIONS.get(
            (kind, action), self.ROLE_ACTIONS.get(action, ("admin",))
        )
        self._ensure_role(actor, allowed_roles)
        self._require(data, self.ACTION_REQUIRED.get((kind, action), ()))
        custom = CUSTOM_TRANSITIONS.get((kind, action))
        extra = custom(actor, entity, data, lookup) if custom else {}
        patch = dict(data)
        if extra:
            patch.update(extra)
        return next_status, patch


def _find_one(lookup, kind, field, value):
    if lookup is None:
        return None
    rows = lookup(kind, field, value) or []
    return rows[0] if rows else None


def _date_ordinal(value):
    return datetime.fromisoformat(str(value)[:10]).date().toordinal()

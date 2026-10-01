import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import (
    Actor,
    ConflictError,
    DomainError,
    InvalidTransition,
    PermissionDenied,
    ValidationError,
)
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class FlakyRepository(SQLiteRepository):
    """Fails the next `failures` transactions to simulate write errors."""

    def __init__(self, path):
        super().__init__(path)
        self.failures = 0

    def run_in_transaction(self, work):
        if self.failures:
            self.failures -= 1
            raise sqlite3.OperationalError("simulated write failure")
        return super().run_in_transaction(work)


class GovernanceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.admin = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def _issued_grant(self, capacity=1, expires="2099-01-01"):
        dataset = self.service.create(
            self.admin,
            "dataset",
            {"name": "Cohort", "access_policy": "controlled", "capacity": capacity},
        )
        application = self.service.create(
            self.admin,
            "application",
            {"dataset_id": dataset["id"], "applicant_id": "APP-1", "purpose": "research"},
        )
        self.service.transition(self.admin, application["id"], "submit", {})
        self.service.transition(self.admin, application["id"], "review", {"committee_id": "c1"})
        self.service.transition(
            self.admin,
            application["id"],
            "approve",
            {"approvals": ["r1", "r2", "r3"], "terms": "noncommercial", "expires_at": expires},
        )
        grant = self.service.create(
            self.admin,
            "grant",
            {
                "application_id": application["id"],
                "dataset_id": dataset["id"],
                "recipient": "researcher-1",
            },
        )
        return dataset, grant, expires

    def _active_grant(self, capacity=1, expires="2099-01-01"):
        dataset, grant, expires = self._issued_grant(capacity, expires)
        self.service.transition(
            self.admin,
            grant["id"],
            "activate",
            {"starts_at": "2026-01-01", "expires_at": expires},
        )
        return dataset, grant

    def _started_session(self, grant):
        session = self.service.create(self.admin, "session", {"grant_id": grant["id"]})
        return self.service.transition(self.admin, session["id"], "start", {})

    def test_capacity_full_queues_and_returns_release_time(self):
        dataset, grant = self._active_grant(capacity=1)
        first = self._started_session(grant)
        self.assertEqual(first["status"], "active")
        self.assertEqual(first["data"]["lease_expires_at"], "2099-01-01")

        second = self.service.create(self.admin, "session", {"grant_id": grant["id"]})
        queued = self.service.transition(self.admin, second["id"], "start", {})
        self.assertEqual(queued["status"], "queued")
        # 回传释放时间：最早空出槽位的时刻
        self.assertEqual(queued["data"]["release_at"], "2099-01-01")

        self.service.transition(self.admin, first["id"], "release", {})
        promoted = self.service.transition(self.admin, second["id"], "start", {})
        self.assertEqual(promoted["status"], "active")
        self.assertNotIn("release_at", promoted["data"])

    def test_revoke_freezes_queued_and_stops_active_keeping_records(self):
        dataset, grant = self._active_grant(capacity=1)
        active = self._started_session(grant)
        self.service.transition(self.admin, active["id"], "record", {"note": "read variants 1-100"})
        queued = self.service.create(self.admin, "session", {"grant_id": grant["id"]})
        self.service.transition(self.admin, queued["id"], "start", {})

        result = self.service.transition(self.admin, grant["id"], "revoke", {"reason": "misuse"})
        self.assertEqual(result["status"], "revoked")
        reconciled = {item["id"]: item["to"] for item in result["reconciled_sessions"]}
        self.assertEqual(reconciled, {active["id"]: "stopped", queued["id"]: "frozen"})

        stopped = self.service.get(active["id"])
        self.assertEqual(stopped["status"], "stopped")
        # 已开始的会话被停止，但取用记录保留
        records = stopped["data"]["access_records"]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["note"], "read variants 1-100")
        frozen = self.service.get(queued["id"])
        self.assertEqual(frozen["status"], "frozen")

        # 对账写入审计时间线
        self.assertEqual(self.service.audit_log(active["id"])[-1]["action"], "stop")
        self.assertEqual(self.service.audit_log(queued["id"])[-1]["action"], "freeze")
        # 停止后的会话不能继续读数据
        with self.assertRaises(InvalidTransition):
            self.service.transition(self.admin, active["id"], "record", {"note": "more"})

    def test_expire_reconciles_sessions(self):
        dataset, grant = self._active_grant(capacity=2)
        started = self._started_session(grant)
        waiting = self.service.create(self.admin, "session", {"grant_id": grant["id"]})

        result = self.service.transition(
            self.admin, grant["id"], "expire", {"expired_at": "2026-10-01"}
        )
        self.assertEqual(result["status"], "expired")
        self.assertEqual(self.service.get(started["id"])["status"], "stopped")
        self.assertEqual(self.service.get(waiting["id"])["status"], "frozen")

    def test_concurrent_grant_activation_allows_only_one(self):
        dataset, grant, expires = self._issued_grant()
        results = []
        errors = []

        def activate(user):
            try:
                results.append(
                    self.service.transition(
                        Actor(user, "committee"),
                        grant["id"],
                        "activate",
                        {"starts_at": "2026-01-01", "expires_at": expires},
                    )
                )
            except DomainError as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=activate, args=("officer-a",)),
            threading.Thread(target=activate, args=("officer-b",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        self.assertEqual(self.service.get(grant["id"])["status"], "active")

    def test_concurrent_session_start_never_exceeds_capacity(self):
        dataset, grant = self._active_grant(capacity=1)
        sessions = [
            self.service.create(self.admin, "session", {"grant_id": grant["id"]})
            for _ in range(2)
        ]
        outcomes = []

        def start(session):
            outcomes.append(self.service.transition(self.admin, session["id"], "start", {}))

        threads = [threading.Thread(target=start, args=(item,)) for item in sessions]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(sorted(item["status"] for item in outcomes), ["active", "queued"])

    def test_retry_after_write_failure_resumes_from_last_progress(self):
        dataset, grant = self._active_grant(capacity=1)
        active = self._started_session(grant)
        waiting = self.service.create(self.admin, "session", {"grant_id": grant["id"]})

        flaky = FlakyRepository(Path(self.tmp.name) / "test.db")
        service = DomainService(flaky, RuleEngine())
        flaky.failures = 1
        with self.assertRaises(sqlite3.OperationalError):
            service.transition(
                self.admin, grant["id"], "revoke", {"reason": "misuse"}, idempotency_key="revoke-1"
            )
        # 失败整体回滚，没有半提交的进度
        self.assertEqual(service.get(grant["id"])["status"], "active")
        self.assertEqual(service.get(active["id"])["status"], "active")

        # 同一个幂等键重试，从上次进度继续
        result = service.transition(
            self.admin, grant["id"], "revoke", {"reason": "misuse"}, idempotency_key="revoke-1"
        )
        self.assertEqual(result["status"], "revoked")
        self.assertEqual(service.get(active["id"])["status"], "stopped")
        self.assertEqual(service.get(waiting["id"])["status"], "frozen")

        # 重放同一请求直接返回已提交结果，不重复执行
        replay = service.transition(
            self.admin, grant["id"], "revoke", {"reason": "misuse"}, idempotency_key="revoke-1"
        )
        self.assertEqual(replay["status"], "revoked")
        revokes = [a for a in service.audit_log(grant["id"]) if a["action"] == "revoke"]
        self.assertEqual(len(revokes), 1)
        stops = [a for a in service.audit_log(active["id"]) if a["action"] == "stop"]
        self.assertEqual(len(stops), 1)
        # 幂等键不能挪用到别的动作
        with self.assertRaises(ConflictError):
            service.transition(
                self.admin, grant["id"], "expire", {"expired_at": "2026-10-01"}, idempotency_key="revoke-1"
            )

    def test_unauthorized_capacity_change_rejected(self):
        dataset, grant = self._active_grant()
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                Actor("app-1", "applicant"), dataset["id"], "set_capacity", {"capacity": 5}
            )
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                Actor("aud-1", "auditor"), dataset["id"], "set_capacity", {"capacity": 5}
            )
        with self.assertRaises(ValidationError):
            self.service.transition(self.admin, dataset["id"], "set_capacity", {"capacity": 0})
        updated = self.service.transition(
            Actor("officer", "committee"), dataset["id"], "set_capacity", {"capacity": 3}
        )
        self.assertEqual(updated["data"]["capacity"], 3)
        self.assertEqual(updated["status"], "registered")

    def test_session_requires_active_grant(self):
        dataset, grant, expires = self._issued_grant()
        with self.assertRaises(ValidationError):
            self.service.create(self.admin, "session", {"grant_id": grant["id"]})

        dataset, grant = self._active_grant()
        session = self.service.create(self.admin, "session", {"grant_id": grant["id"]})
        self.service.transition(self.admin, grant["id"], "revoke", {"reason": "purpose changed"})
        # 撤回对账后未开始的会话被冻结，不能再启动
        self.assertEqual(self.service.get(session["id"])["status"], "frozen")
        with self.assertRaises(InvalidTransition):
            self.service.transition(self.admin, session["id"], "start", {})


if __name__ == "__main__":
    unittest.main()

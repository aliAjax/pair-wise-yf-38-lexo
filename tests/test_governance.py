import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, InvalidTransition, PermissionDenied, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class GovernanceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.admin = Actor("admin", "admin")
        self.viewer = Actor("viewer", "viewer")

    def tearDown(self):
        self.tmp.cleanup()

    def _dataset(self, capacity):
        return self.service.create(
            self.admin,
            "dataset",
            {"name": "controlled-cohort", "access_policy": "controlled", "capacity": capacity},
        )

    def _grant(self, dataset_id, recipient):
        application = self.service.create(
            self.admin,
            "application",
            {"dataset_id": dataset_id, "applicant_id": "APP-1", "purpose": "variant analysis"},
        )
        return self.service.create(
            self.admin,
            "grant",
            {
                "application_id": application["id"],
                "dataset_id": dataset_id,
                "recipient": recipient,
            },
        )

    def _activate(self, grant_id, expected_version=None):
        return self.service.transition(
            self.admin,
            grant_id,
            "activate",
            {"starts_at": "2026-01-01", "expires_at": "2099-12-31"},
            expected_version,
        )

    def test_capacity_queue_and_release_time(self):
        dataset = self._dataset(2)
        g1 = self._grant(dataset["id"], "researcher-1")
        g2 = self._grant(dataset["id"], "researcher-2")
        g3 = self._grant(dataset["id"], "researcher-3")

        r1 = self._activate(g1["id"])
        r2 = self._activate(g2["id"])
        r3 = self._activate(g3["id"])

        self.assertEqual(r1["session"]["status"], "active")
        self.assertEqual(r2["session"]["status"], "active")
        self.assertTrue(r3["queued"])
        self.assertEqual(r3["session"]["status"], "queued")
        self.assertIsNotNone(r3["release_time"])
        # release time is the earliest active session end
        self.assertEqual(r3["release_time"], "2099-12-31")

    def test_revoke_stops_active_and_drains_queue(self):
        dataset = self._dataset(1)
        g1 = self._grant(dataset["id"], "researcher-1")
        g2 = self._grant(dataset["id"], "researcher-2")
        r1 = self._activate(g1["id"])
        r2 = self._activate(g2["id"])
        self.assertEqual(r1["session"]["status"], "active")
        self.assertEqual(r2["session"]["status"], "queued")

        self.service.transition(self.admin, g1["id"], "revoke", {"reason": "purpose changed"})

        s1 = self.service.get(r1["session"]["id"])
        s2 = self.service.get(r2["session"]["id"])
        self.assertEqual(s1["status"], "stopped")
        self.assertEqual(s2["status"], "active")

    def test_revoke_freezes_queued_session(self):
        dataset = self._dataset(1)
        g1 = self._grant(dataset["id"], "researcher-1")
        g2 = self._grant(dataset["id"], "researcher-2")
        r1 = self._activate(g1["id"])
        r2 = self._activate(g2["id"])
        self.assertEqual(r2["session"]["status"], "queued")

        self.service.transition(self.admin, g2["id"], "revoke", {"reason": "withdrawn"})

        s2 = self.service.get(r2["session"]["id"])
        s1 = self.service.get(r1["session"]["id"])
        self.assertEqual(s2["status"], "frozen")
        self.assertEqual(s1["status"], "active")

    def test_expire_reconciles_like_revoke(self):
        dataset = self._dataset(1)
        g1 = self._grant(dataset["id"], "researcher-1")
        g2 = self._grant(dataset["id"], "researcher-2")
        r1 = self._activate(g1["id"])
        r2 = self._activate(g2["id"])

        self.service.transition(self.admin, g1["id"], "expire", {"expired_at": "2099-12-31"})

        s1 = self.service.get(r1["session"]["id"])
        s2 = self.service.get(r2["session"]["id"])
        self.assertEqual(s1["status"], "stopped")
        self.assertEqual(s2["status"], "active")

    def test_audit_records_preserved_after_stop(self):
        dataset = self._dataset(1)
        g1 = self._grant(dataset["id"], "researcher-1")
        r1 = self._activate(g1["id"])

        self.service.transition(self.admin, g1["id"], "revoke", {"reason": "done"})

        logs = self.service.audit_log(r1["session"]["id"])
        actions = [log["action"] for log in logs]
        self.assertIn("session_created", actions)
        self.assertIn("stop", actions)
        # audit log is append-only: nothing deleted
        self.assertGreaterEqual(len(logs), 2)

    def test_concurrent_activate_only_one_succeeds(self):
        dataset = self._dataset(2)
        g1 = self._grant(dataset["id"], "researcher-1")
        barrier = threading.Barrier(2)
        results = []
        errors = []

        def worker():
            barrier.wait()
            try:
                results.append(self._activate(g1["id"]))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # 两人同时提交同一 grant 的激活：只放行一个
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        self.assertEqual(results[0]["session"]["status"], "active")
        sessions = self.service.list("session")
        grant_sessions = [s for s in sessions if s["data"].get("grant_id") == g1["id"]]
        self.assertEqual(len(grant_sessions), 1)

    def test_optimistic_lock_rejects_second_activation(self):
        # 确定性地验证乐观锁：两个请求都读到版本 1，只有一个能提交
        dataset = self._dataset(2)
        g1 = self._grant(dataset["id"], "researcher-1")
        grant = self.repo.get_entity(g1["id"])
        self.assertEqual(grant["version"], 1)
        merged = dict(grant["data"])
        merged.update(
            {"starts_at": "2026-01-01", "expires_at": "2099-12-31", "activated_by": self.admin.user_id}
        )
        first = self.repo.update_entity(g1["id"], 1, "active", merged)
        self.assertEqual(first["status"], "active")
        with self.assertRaises(ConflictError):
            self.repo.update_entity(g1["id"], 1, "active", merged)

    def test_duplicate_activate_is_rejected(self):
        dataset = self._dataset(2)
        g1 = self._grant(dataset["id"], "researcher-1")
        self._activate(g1["id"])
        with self.assertRaises(InvalidTransition):
            self._activate(g1["id"])
        sessions = self.service.list("session")
        grant_sessions = [s for s in sessions if s["data"].get("grant_id") == g1["id"]]
        self.assertEqual(len(grant_sessions), 1)

    def test_activate_retries_from_last_progress(self):
        dataset = self._dataset(2)
        g1 = self._grant(dataset["id"], "researcher-1")
        # Simulate a crash after the session was persisted but before the grant update:
        # pre-create the session, then activate must reuse it instead of starting over.
        session = self.service._ensure_session(self.repo.get_entity(g1["id"]), self.admin)
        self.assertEqual(session["status"], "active")

        result = self._activate(g1["id"])
        self.assertEqual(result["session"]["id"], session["id"])
        sessions = self.service.list("session")
        grant_sessions = [s for s in sessions if s["data"].get("grant_id") == g1["id"]]
        self.assertEqual(len(grant_sessions), 1)
        self.assertEqual(result["status"], "active")

    def test_set_capacity_requires_permission(self):
        dataset = self._dataset(2)
        with self.assertRaises(PermissionDenied):
            self.service.transition(self.viewer, dataset["id"], "set_capacity", {"capacity": 5})
        updated = self.service.transition(
            self.admin, dataset["id"], "set_capacity", {"capacity": 5}
        )
        self.assertEqual(updated["data"]["capacity"], 5)

    def test_set_capacity_validates_positive_integer(self):
        dataset = self._dataset(2)
        for bad in (0, -1, "x", True):
            with self.assertRaises(ValidationError):
                self.service.transition(
                    self.admin, dataset["id"], "set_capacity", {"capacity": bad}
                )

    def test_default_capacity_is_effectively_unlimited(self):
        dataset = self.service.create(
            self.admin, "dataset", {"name": "D", "access_policy": "controlled"}
        )
        self.assertGreaterEqual(dataset["data"]["capacity"], 1_000_000)
        grant = self._grant(dataset["id"], "researcher-1")
        result = self._activate(grant["id"])
        self.assertEqual(result["session"]["status"], "active")


if __name__ == "__main__":
    unittest.main()

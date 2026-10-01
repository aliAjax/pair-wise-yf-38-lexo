import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class FailureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())

    def tearDown(self):
        self.tmp.cleanup()

    def test_permission_denied(self):
        entity = self.service.create(
            Actor("admin", "admin"), 'dataset', {'name': 'D', 'access_policy': 'controlled'}
        )
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                Actor("viewer", "viewer"),
                entity["id"],
                'restrict',
                {'reason': 'review'},
            )

    def test_version_conflict(self):
        entity = self.service.create(
            Actor("admin", "admin"), 'dataset', {'name': 'D', 'access_policy': 'controlled'}
        )
        with self.assertRaises(ConflictError):
            self.service.transition(
                Actor("admin", "admin"),
                entity["id"],
                'restrict',
                {'reason': 'review'},
                expected_version=999,
            )

    def test_duplicate_idempotency_key_returns_same_entity(self):
        first = self.service.create(
            Actor("admin", "admin"),
            'dataset',
            {'name': 'D', 'access_policy': 'controlled'},
            idempotency_key="duplicate-check",
        )
        second = self.service.create(
            Actor("admin", "admin"),
            'dataset',
            {'name': 'D', 'access_policy': 'controlled'},
            idempotency_key="duplicate-check",
        )
        self.assertEqual(first["id"], second["id"])


if __name__ == "__main__":
    unittest.main()

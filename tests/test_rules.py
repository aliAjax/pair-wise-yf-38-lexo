import unittest

from src.rules import valid_grant_window
from src.domain import Actor, PermissionDenied, ValidationError
from src.rules import RuleEngine


class RulesTest(unittest.TestCase):
    def setUp(self):
        self.rules = RuleEngine()
        self.admin = Actor("rule-tester", "admin")

    def test_rule_calculation_or_validation(self):
        self.assertTrue(valid_grant_window("2099-01-01", "2026-09-24"))
        self.assertFalse(valid_grant_window("2026-01-01", "2026-09-24"))
        with self.assertRaises(ValidationError):
            self.rules.validate_transition(self.admin, {"kind": "application", "status": "under_review", "data": {}}, "approve", {"approvals": ["a"], "terms": "x", "expires_at": "2099-01-01"})


if __name__ == "__main__":
    unittest.main()

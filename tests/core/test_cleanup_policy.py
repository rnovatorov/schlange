import unittest
import datetime

import schlange


class TestCleanupPolicy(unittest.TestCase):
    """Test cases for schlange.core.CleanupPolicy"""

    def test_succeeded_deadline(self):
        """Test succeeded deadline calculation"""
        policy = schlange.core.CleanupPolicy(
            delete_succeeded_after=3600,  # 1 hour
            delete_failed_after=86400,  # 1 day
        )
        now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        deadline = policy.succeeded_deadline(now)
        expected = datetime.datetime(2025, 1, 1, 11, 0, 0, tzinfo=datetime.UTC)
        self.assertEqual(deadline, expected)

    def test_failed_deadline(self):
        """Test failed deadline calculation"""
        policy = schlange.core.CleanupPolicy(
            delete_succeeded_after=3600,  # 1 hour
            delete_failed_after=86400,  # 1 day
        )
        now = datetime.datetime(2025, 1, 2, 12, 0, 0, tzinfo=datetime.UTC)
        deadline = policy.failed_deadline(now)
        expected = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.assertEqual(deadline, expected)

    def test_zero_cleanup_time(self):
        """Test with zero cleanup time"""
        policy = schlange.core.CleanupPolicy(
            delete_succeeded_after=0,
            delete_failed_after=0,
        )
        now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.assertEqual(policy.succeeded_deadline(now), now)
        self.assertEqual(policy.failed_deadline(now), now)


if __name__ == "__main__":
    unittest.main()

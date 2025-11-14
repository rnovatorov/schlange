import unittest

import schlange


class TestRetryPolicy(unittest.TestCase):
    """Test cases for schlange.RetryPolicy"""

    def test_delay_with_zero_attempts(self):
        """Test delay calculation with 0 attempts"""
        policy = schlange.RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=100.0,
            max_attempts=5,
        )
        self.assertEqual(policy.delay(0), 0)

    def test_delay_with_one_attempt(self):
        """Test delay calculation with 1 attempt returns initial delay"""
        policy = schlange.RetryPolicy(
            initial_delay=5.0,
            backoff_factor=2.0,
            max_delay=100.0,
            max_attempts=5,
        )
        self.assertEqual(policy.delay(1), 5.0)

    def test_delay_with_exponential_backoff(self):
        """Test exponential backoff calculation"""
        policy = schlange.RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=None,
            max_attempts=5,
        )
        # Attempt 1: 1.0
        # Attempt 2: 1.0 * 2.0 = 2.0
        # Attempt 3: 2.0 * 2.0 = 4.0
        self.assertEqual(policy.delay(1), 1.0)
        self.assertEqual(policy.delay(2), 2.0)
        self.assertEqual(policy.delay(3), 4.0)
        self.assertEqual(policy.delay(4), 8.0)

    def test_delay_with_max_delay_cap(self):
        """Test that delay is capped at max_delay"""
        policy = schlange.RetryPolicy(
            initial_delay=10.0,
            backoff_factor=2.0,
            max_delay=15.0,
            max_attempts=5,
        )
        # Without cap: 10 * 2 = 20, but should be capped at 15
        self.assertEqual(policy.delay(2), 15.0)

    def test_delay_raises_too_many_attempts(self):
        """Test that schlange.TooManyAttemptsError is raised when max_attempts is exceeded"""
        policy = schlange.RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=100.0,
            max_attempts=3,
        )
        # Should work for attempts 0, 1, 2
        policy.delay(0)
        policy.delay(1)
        policy.delay(2)
        # Should raise for attempt 3
        with self.assertRaises(schlange.TooManyAttemptsError):
            policy.delay(3)

    def test_total_delay(self):
        """Test total delay calculation"""
        policy = schlange.RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=None,
            max_attempts=4,
        )
        # 0 + 1 + 2 + 4 = 7
        self.assertEqual(policy.total_delay(), 7.0)

    def test_total_delay_with_max_delay(self):
        """Test total delay calculation with max_delay cap"""
        policy = schlange.RetryPolicy(
            initial_delay=10.0,
            backoff_factor=2.0,
            max_delay=15.0,
            max_attempts=4,
        )
        # 0 + 10 + 15 + 15 = 40
        self.assertEqual(policy.total_delay(), 40.0)


if __name__ == "__main__":
    unittest.main()

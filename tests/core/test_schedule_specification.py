import unittest
import datetime

import schlange


class TestScheduleSpecification(unittest.TestCase):
    """Test cases for schlange.core.ScheduleSpecification"""

    def setUp(self):
        """Set up common test data"""
        self.now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.retry_policy = schlange.core.RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=60.0,
            max_attempts=3,
        )
        self.task_retry_policy = schlange.core.RetryPolicy(
            initial_delay=2.0,
            backoff_factor=2.0,
            max_delay=120.0,
            max_attempts=5,
        )

    def test_empty_specification_matches_any_schedule(self):
        """Test that empty specification matches any schedule"""
        spec = schlange.core.ScheduleSpecification()
        
        schedule = schlange.core.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        self.assertTrue(spec.is_satisfied_by(schedule))

    def test_enabled_specification(self):
        """Test filtering by enabled state"""
        enabled_spec = schlange.core.ScheduleSpecification(enabled=True)
        disabled_spec = schlange.core.ScheduleSpecification(enabled=False)
        
        enabled_schedule = schlange.core.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        self.assertTrue(enabled_spec.is_satisfied_by(enabled_schedule))
        self.assertFalse(disabled_spec.is_satisfied_by(enabled_schedule))
        
        disabled_schedule = schlange.core.Schedule.create(
            now=self.now,
            id="schedule-2",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=False,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        self.assertFalse(enabled_spec.is_satisfied_by(disabled_schedule))
        self.assertTrue(disabled_spec.is_satisfied_by(disabled_schedule))

    def test_ready_as_of_specification(self):
        """Test filtering by ready_as_of time"""
        check_time = self.now + datetime.timedelta(seconds=10)
        spec = schlange.core.ScheduleSpecification(ready_as_of=check_time)
        
        # schlange.core.Schedule ready in 5 seconds (ready before check_time)
        early_schedule = schlange.core.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=5.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        self.assertTrue(spec.is_satisfied_by(early_schedule))
        
        # schlange.core.Schedule ready in 15 seconds (not ready by check_time)
        late_schedule = schlange.core.Schedule.create(
            now=self.now,
            id="schedule-2",
            delay=15.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        self.assertFalse(spec.is_satisfied_by(late_schedule))

    def test_combined_specifications(self):
        """Test combining multiple specification criteria"""
        check_time = self.now + datetime.timedelta(seconds=10)
        spec = schlange.core.ScheduleSpecification(
            enabled=True,
            ready_as_of=check_time,
        )
        
        # schlange.core.Schedule that matches both criteria
        matching_schedule = schlange.core.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=5.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        self.assertTrue(spec.is_satisfied_by(matching_schedule))
        
        # schlange.core.Schedule that's disabled
        disabled_schedule = schlange.core.Schedule.create(
            now=self.now,
            id="schedule-2",
            delay=5.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=False,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        self.assertFalse(spec.is_satisfied_by(disabled_schedule))
        
        # schlange.core.Schedule that's not ready yet
        not_ready_schedule = schlange.core.Schedule.create(
            now=self.now,
            id="schedule-3",
            delay=15.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        self.assertFalse(spec.is_satisfied_by(not_ready_schedule))


if __name__ == "__main__":
    unittest.main()

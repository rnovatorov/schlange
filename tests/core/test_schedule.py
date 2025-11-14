import unittest
import datetime
import uuid

import schlange


class TestSchedule(unittest.TestCase):
    """Test cases for schlange.Schedule domain model"""

    def setUp(self):
        """Set up common test data"""
        self.now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.retry_policy = schlange.RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=60.0,
            max_attempts=3,
        )
        self.task_retry_policy = schlange.RetryPolicy(
            initial_delay=2.0,
            backoff_factor=2.0,
            max_delay=120.0,
            max_attempts=5,
        )

    def test_create_schedule(self):
        """Test creating a new schedule"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=10.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={"key": "value"},
            task_retry_policy=self.task_retry_policy,
        )
        
        self.assertEqual(schedule.id, "schedule-1")
        self.assertEqual(schedule.version, 1)
        self.assertEqual(schedule.created_at, self.now)
        origin = self.now + datetime.timedelta(seconds=10.0)
        self.assertEqual(schedule.origin, origin)
        self.assertEqual(schedule.ready_at, origin)
        self.assertEqual(schedule.interval, 60.0)
        self.assertEqual(schedule.retry_policy, self.retry_policy)
        self.assertTrue(schedule.enabled)
        self.assertEqual(schedule.task_args, {"key": "value"})
        self.assertEqual(schedule.task_retry_policy, self.task_retry_policy)
        self.assertEqual(schedule.task_sequence_number, 1)
        self.assertEqual(schedule.firings, [])

    def test_create_schedule_disabled(self):
        """Test creating a disabled schedule"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=False,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        self.assertFalse(schedule.enabled)

    def test_ready_when_time_has_come(self):
        """Test schedule is ready when ready_at time has passed"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=10.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        # 9 seconds later - not ready yet
        self.assertFalse(schedule.ready(self.now + datetime.timedelta(seconds=9)))
        
        # 10 seconds later - ready
        self.assertTrue(schedule.ready(self.now + datetime.timedelta(seconds=10)))
        
        # 11 seconds later - still ready
        self.assertTrue(schedule.ready(self.now + datetime.timedelta(seconds=11)))

    def test_generate_task_id(self):
        """Test task ID generation"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        task_id_1 = schedule.generate_task_id()
        # Should be a valid UUID
        uuid.UUID(task_id_1)
        
        # Same sequence number should generate same ID
        task_id_2 = schedule.generate_task_id()
        self.assertEqual(task_id_1, task_id_2)
        
        # Different sequence number should generate different ID
        schedule.task_sequence_number = 2
        task_id_3 = schedule.generate_task_id()
        self.assertNotEqual(task_id_1, task_id_3)

    def test_begin_firing(self):
        """Test beginning schedule firing"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        schedule.begin_firing(self.now)
        
        self.assertEqual(len(schedule.firings), 1)
        self.assertEqual(schedule.firings[0].begun_at, self.now)
        self.assertEqual(schedule.firings[0].task_sequence_number, 1)
        self.assertFalse(schedule.firings[0].ended)

    def test_begin_firing_when_disabled(self):
        """Test that beginning firing fails if schedule is disabled"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=False,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        with self.assertRaises(schlange.ScheduleNotEnabledError):
            schedule.begin_firing(self.now)

    def test_begin_firing_when_not_ready(self):
        """Test that beginning firing fails if schedule is not ready"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=10.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        with self.assertRaises(schlange.ScheduleNotReadyError):
            schedule.begin_firing(self.now)

    def test_begin_firing_when_previous_not_ended(self):
        """Test that beginning firing fails if previous firing hasn't ended"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        schedule.begin_firing(self.now)
        
        with self.assertRaises(schlange.ScheduleFiringNotEndedYetError):
            schedule.begin_firing(self.now)

    def test_end_firing_success(self):
        """Test ending firing successfully"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        schedule.begin_firing(self.now)
        end_time = self.now + datetime.timedelta(seconds=5)
        schedule.end_firing(end_time, error=None)
        
        self.assertTrue(schedule.firings[0].ended)
        self.assertIsNone(schedule.firings[0].error)
        # Sequence number should increment
        self.assertEqual(schedule.task_sequence_number, 2)
        # Origin should advance by interval
        self.assertEqual(
            schedule.origin,
            self.now + datetime.timedelta(seconds=60.0)
        )
        # Ready time should be at next origin
        self.assertEqual(schedule.ready_at, schedule.origin)

    def test_end_firing_with_error_and_retry(self):
        """Test ending firing with error triggers retry"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        schedule.begin_firing(self.now)
        end_time = self.now + datetime.timedelta(seconds=5)
        schedule.end_firing(end_time, error="Failed to create task")
        
        self.assertTrue(schedule.firings[0].ended)
        self.assertEqual(schedule.firings[0].error, "Failed to create task")
        # Should not increment sequence number yet (will retry)
        self.assertEqual(schedule.task_sequence_number, 1)
        # Ready time should be retry time (initial_delay = 1.0 second)
        expected_retry_time = end_time + datetime.timedelta(seconds=1.0)
        self.assertEqual(schedule.ready_at, expected_retry_time)

    def test_end_firing_with_error_max_attempts(self):
        """Test that schedule moves to next firing after max retry attempts"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        # Attempt 1
        schedule.begin_firing(self.now)
        schedule.end_firing(self.now + datetime.timedelta(seconds=1), error="Error 1")
        self.assertEqual(schedule.task_sequence_number, 1)
        
        # Attempt 2
        schedule.begin_firing(self.now + datetime.timedelta(seconds=2))
        schedule.end_firing(self.now + datetime.timedelta(seconds=3), error="Error 2")
        self.assertEqual(schedule.task_sequence_number, 1)
        
        # Attempt 3 (max_attempts reached)
        schedule.begin_firing(self.now + datetime.timedelta(seconds=5))
        schedule.end_firing(self.now + datetime.timedelta(seconds=6), error="Error 3")
        # Should move to next sequence
        self.assertEqual(schedule.task_sequence_number, 2)

    def test_end_firing_not_begun(self):
        """Test that ending firing fails if not begun"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        with self.assertRaises(schlange.ScheduleFiringNotBegunYetError):
            schedule.end_firing(self.now, error=None)

    def test_last_firing(self):
        """Test last_firing property"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        self.assertIsNone(schedule.last_firing)
        
        schedule.begin_firing(self.now)
        self.assertIsNotNone(schedule.last_firing)
        self.assertEqual(schedule.last_firing.begun_at, self.now)

    def test_firings_reset_on_sequence_change(self):
        """Test that firings are reset when sequence number changes"""
        schedule = schlange.Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        # First firing succeeds
        schedule.begin_firing(self.now)
        schedule.end_firing(self.now + datetime.timedelta(seconds=1), error=None)
        self.assertEqual(schedule.task_sequence_number, 2)
        
        # Start next firing with new sequence number
        schedule.begin_firing(self.now + datetime.timedelta(seconds=61))
        # Firings should be reset
        self.assertEqual(len(schedule.firings), 1)
        self.assertEqual(schedule.firings[0].task_sequence_number, 2)


if __name__ == "__main__":
    unittest.main()

import unittest
import datetime
from unittest.mock import Mock, call

from schlange.background.schedule_worker import ScheduleWorker
from schlange.core.schedule import Schedule
from schlange.core.retry_policy import RetryPolicy
from schlange.core.errors import (
    ScheduleNotFoundError,
    ScheduleNotEnabledError,
    ScheduleNotReadyError,
)


class TestScheduleWorker(unittest.TestCase):
    """Test cases for ScheduleWorker"""

    def setUp(self):
        """Set up test fixtures"""
        self.schedule_service = Mock()
        self.now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.retry_policy = RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=60.0,
            max_attempts=3,
        )
        self.task_retry_policy = RetryPolicy(
            initial_delay=2.0,
            backoff_factor=2.0,
            max_delay=120.0,
            max_attempts=5,
        )

    def test_initialization(self):
        """Test ScheduleWorker initialization"""
        worker = ScheduleWorker(
            interval=1.0,
            schedule_service=self.schedule_service,
        )
        
        self.assertEqual(worker.interval, 1.0)
        self.assertEqual(worker.schedule_service, self.schedule_service)

    def test_work_with_no_schedules(self):
        """Test work method when no schedules are fireable"""
        self.schedule_service.fireable_schedules.return_value = []
        
        worker = ScheduleWorker(
            interval=1.0,
            schedule_service=self.schedule_service,
        )
        
        worker.work()
        
        self.schedule_service.fireable_schedules.assert_called_once()
        self.schedule_service.fire_schedule.assert_not_called()

    def test_work_fires_schedules(self):
        """Test work method fires eligible schedules"""
        schedule1 = Schedule.create(
            now=self.now - datetime.timedelta(seconds=10),
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={"key": "value1"},
            task_retry_policy=self.task_retry_policy,
        )
        
        schedule2 = Schedule.create(
            now=self.now - datetime.timedelta(seconds=10),
            id="schedule-2",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={"key": "value2"},
            task_retry_policy=self.task_retry_policy,
        )
        
        # First call returns schedules, second call returns empty
        self.schedule_service.fireable_schedules.side_effect = [
            [schedule1, schedule2],
            [],
        ]
        
        # Mock fire_schedule to return completed schedules
        def fire_side_effect(schedule_id):
            for s in [schedule1, schedule2]:
                if s.id == schedule_id:
                    s.begin_firing(self.now)
                    s.end_firing(self.now + datetime.timedelta(seconds=1), error=None)
                    return s
            raise ScheduleNotFoundError()
        
        self.schedule_service.fire_schedule.side_effect = fire_side_effect
        
        worker = ScheduleWorker(
            interval=1.0,
            schedule_service=self.schedule_service,
        )
        
        worker.work()
        
        # Verify fireable_schedules was called twice
        self.assertEqual(self.schedule_service.fireable_schedules.call_count, 2)
        
        # Verify schedules were fired
        self.assertEqual(self.schedule_service.fire_schedule.call_count, 2)
        self.schedule_service.fire_schedule.assert_any_call("schedule-1")
        self.schedule_service.fire_schedule.assert_any_call("schedule-2")

    def test_fire_schedule_success(self):
        """Test successful schedule firing"""
        schedule = Schedule.create(
            now=self.now - datetime.timedelta(seconds=10),
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        # Mock fire_schedule to return completed schedule
        schedule.begin_firing(self.now)
        schedule.end_firing(self.now + datetime.timedelta(seconds=1), error=None)
        self.schedule_service.fire_schedule.return_value = schedule
        
        worker = ScheduleWorker(
            interval=1.0,
            schedule_service=self.schedule_service,
        )
        
        worker._fire_schedule(schedule)
        
        self.schedule_service.fire_schedule.assert_called_once_with("schedule-1")

    def test_fire_schedule_handles_not_found_error(self):
        """Test that ScheduleNotFoundError is handled gracefully"""
        schedule = Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        self.schedule_service.fire_schedule.side_effect = ScheduleNotFoundError()
        
        worker = ScheduleWorker(
            interval=1.0,
            schedule_service=self.schedule_service,
        )
        
        # Should not raise exception
        worker._fire_schedule(schedule)
        
        self.schedule_service.fire_schedule.assert_called_once_with("schedule-1")

    def test_fire_schedule_handles_not_enabled_error(self):
        """Test that ScheduleNotEnabledError is handled gracefully"""
        schedule = Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=False,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        self.schedule_service.fire_schedule.side_effect = ScheduleNotEnabledError()
        
        worker = ScheduleWorker(
            interval=1.0,
            schedule_service=self.schedule_service,
        )
        
        # Should not raise exception
        worker._fire_schedule(schedule)
        
        self.schedule_service.fire_schedule.assert_called_once_with("schedule-1")

    def test_fire_schedule_handles_not_ready_error(self):
        """Test that ScheduleNotReadyError is handled gracefully"""
        schedule = Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=10.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        self.schedule_service.fire_schedule.side_effect = ScheduleNotReadyError()
        
        worker = ScheduleWorker(
            interval=1.0,
            schedule_service=self.schedule_service,
        )
        
        # Should not raise exception
        worker._fire_schedule(schedule)
        
        self.schedule_service.fire_schedule.assert_called_once_with("schedule-1")

    def test_fire_schedule_handles_io_error(self):
        """Test that IOError is handled gracefully"""
        schedule = Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        self.schedule_service.fire_schedule.side_effect = IOError("Database error")
        
        worker = ScheduleWorker(
            interval=1.0,
            schedule_service=self.schedule_service,
        )
        
        # Should not raise exception
        worker._fire_schedule(schedule)
        
        self.schedule_service.fire_schedule.assert_called_once_with("schedule-1")

    def test_work_stops_when_stopping_event_set(self):
        """Test that work loop stops when stopping event is set"""
        schedule = Schedule.create(
            now=self.now,
            id="schedule-1",
            delay=0.0,
            interval=60.0,
            retry_policy=self.retry_policy,
            enabled=True,
            task_args={},
            task_retry_policy=self.task_retry_policy,
        )
        
        # Return schedules indefinitely
        self.schedule_service.fireable_schedules.return_value = [schedule]
        self.schedule_service.fire_schedule.return_value = schedule
        
        worker = ScheduleWorker(
            interval=1.0,
            schedule_service=self.schedule_service,
        )
        
        # Set stopping event before work
        worker.stopping.set()
        
        worker.work()
        
        # Should call fireable_schedules once and then stop
        self.schedule_service.fireable_schedules.assert_called_once()
        self.schedule_service.fire_schedule.assert_not_called()


if __name__ == "__main__":
    unittest.main()

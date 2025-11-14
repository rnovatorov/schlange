import unittest
import datetime

import schlange


class TestTaskSpecification(unittest.TestCase):
    """Test cases for schlange.core.TaskSpecification"""

    def setUp(self):
        """Set up common test data"""
        self.now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.retry_policy = schlange.core.RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=60.0,
            max_attempts=3,
        )

    def test_empty_specification_matches_any_task(self):
        """Test that empty specification matches any task"""
        spec = schlange.core.TaskSpecification()
        
        task = schlange.core.Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        self.assertTrue(spec.is_satisfied_by(task))

    def test_state_specification(self):
        """Test filtering by state"""
        spec = schlange.core.TaskSpecification(state=schlange.core.TaskState.ACTIVE)
        
        active_task = schlange.core.Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        self.assertTrue(spec.is_satisfied_by(active_task))
        
        # Complete the task
        active_task.begin_execution(self.now)
        active_task.end_execution(self.now + datetime.timedelta(seconds=1), error=None)
        self.assertFalse(spec.is_satisfied_by(active_task))

    def test_ready_as_of_specification(self):
        """Test filtering by ready_as_of time"""
        check_time = self.now + datetime.timedelta(seconds=10)
        spec = schlange.core.TaskSpecification(ready_as_of=check_time)
        
        # schlange.core.Task ready in 5 seconds (ready before check_time)
        early_task = schlange.core.Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=5.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        self.assertTrue(spec.is_satisfied_by(early_task))
        
        # schlange.core.Task ready in 15 seconds (not ready by check_time)
        late_task = schlange.core.Task.create(
            now=self.now,
            id="task-2",
            args={},
            delay=15.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        self.assertFalse(spec.is_satisfied_by(late_task))

    def test_last_execution_ended_before_specification(self):
        """Test filtering by last execution end time"""
        deadline = self.now + datetime.timedelta(seconds=100)
        spec = schlange.core.TaskSpecification(last_execution_ended_before=deadline)
        
        # schlange.core.Task with no execution
        no_exec_task = schlange.core.Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        self.assertFalse(spec.is_satisfied_by(no_exec_task))
        
        # schlange.core.Task with execution ended before deadline
        early_task = schlange.core.Task.create(
            now=self.now,
            id="task-2",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        early_task.begin_execution(self.now)
        early_task.end_execution(self.now + datetime.timedelta(seconds=50), error=None)
        self.assertTrue(spec.is_satisfied_by(early_task))
        
        # schlange.core.Task with execution ended after deadline
        late_task = schlange.core.Task.create(
            now=self.now,
            id="task-3",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        late_task.begin_execution(self.now)
        late_task.end_execution(self.now + datetime.timedelta(seconds=150), error=None)
        self.assertFalse(spec.is_satisfied_by(late_task))

    def test_combined_specifications(self):
        """Test combining multiple specification criteria"""
        check_time = self.now + datetime.timedelta(seconds=10)
        spec = schlange.core.TaskSpecification(
            state=schlange.core.TaskState.ACTIVE,
            ready_as_of=check_time,
        )
        
        # schlange.core.Task that matches both criteria
        matching_task = schlange.core.Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=5.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        self.assertTrue(spec.is_satisfied_by(matching_task))
        
        # schlange.core.Task with wrong state
        matching_task.begin_execution(check_time)
        matching_task.end_execution(check_time + datetime.timedelta(seconds=1), error=None)
        self.assertFalse(spec.is_satisfied_by(matching_task))


if __name__ == "__main__":
    unittest.main()

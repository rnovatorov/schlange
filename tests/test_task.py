import unittest
import datetime

from schlange.core.task import Task
from schlange.core.task_state import TaskState
from schlange.core.retry_policy import RetryPolicy
from schlange.core.errors import (
    TaskNotActiveError,
    TaskNotReadyError,
    TaskExecutionNotBegunYetError,
    TaskExecutionNotEndedYetError,
    TaskNotFailedError,
)


class TestTask(unittest.TestCase):
    """Test cases for Task domain model"""

    def setUp(self):
        """Set up common test data"""
        self.now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.retry_policy = RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=60.0,
            max_attempts=3,
        )

    def test_create_task(self):
        """Test creating a new task"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={"key": "value"},
            delay=5.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        self.assertEqual(task.id, "task-1")
        self.assertEqual(task.version, 1)
        self.assertEqual(task.created_at, self.now)
        self.assertEqual(task.state, TaskState.ACTIVE)
        self.assertEqual(task.args, {"key": "value"})
        self.assertEqual(
            task.ready_at,
            self.now + datetime.timedelta(seconds=5.0)
        )
        self.assertEqual(task.retry_policy, self.retry_policy)
        self.assertEqual(task.executions, [])
        self.assertIsNone(task.schedule_id)

    def test_create_task_with_schedule(self):
        """Test creating a task with a schedule_id"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={"key": "value"},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id="schedule-1",
        )
        
        self.assertEqual(task.schedule_id, "schedule-1")

    def test_ready_when_time_has_come(self):
        """Test task is ready when ready_at time has passed"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=5.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        # 4 seconds later - not ready yet
        self.assertFalse(task.ready(self.now + datetime.timedelta(seconds=4)))
        
        # 5 seconds later - ready
        self.assertTrue(task.ready(self.now + datetime.timedelta(seconds=5)))
        
        # 6 seconds later - still ready
        self.assertTrue(task.ready(self.now + datetime.timedelta(seconds=6)))

    def test_begin_execution(self):
        """Test beginning task execution"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        task.begin_execution(self.now)
        
        self.assertEqual(len(task.executions), 1)
        self.assertEqual(task.executions[0].begun_at, self.now)
        self.assertFalse(task.executions[0].ended)

    def test_begin_execution_when_not_active(self):
        """Test that beginning execution fails if task is not active"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        task.state = TaskState.SUCCEEDED
        
        with self.assertRaises(TaskNotActiveError):
            task.begin_execution(self.now)

    def test_begin_execution_when_not_ready(self):
        """Test that beginning execution fails if task is not ready"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=10.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        with self.assertRaises(TaskNotReadyError):
            task.begin_execution(self.now)

    def test_begin_execution_when_previous_not_ended(self):
        """Test that beginning execution fails if previous execution hasn't ended"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        task.begin_execution(self.now)
        
        with self.assertRaises(TaskExecutionNotEndedYetError):
            task.begin_execution(self.now)

    def test_end_execution_success(self):
        """Test ending execution successfully"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        task.begin_execution(self.now)
        end_time = self.now + datetime.timedelta(seconds=5)
        task.end_execution(end_time, error=None)
        
        self.assertEqual(task.state, TaskState.SUCCEEDED)
        self.assertTrue(task.executions[0].ended)
        self.assertIsNone(task.executions[0].error)

    def test_end_execution_with_error_and_retry(self):
        """Test ending execution with error triggers retry"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        task.begin_execution(self.now)
        end_time = self.now + datetime.timedelta(seconds=5)
        task.end_execution(end_time, error="Something went wrong")
        
        # Task should still be active with retry scheduled
        self.assertEqual(task.state, TaskState.ACTIVE)
        self.assertTrue(task.executions[0].ended)
        self.assertEqual(task.executions[0].error, "Something went wrong")
        # First retry should be scheduled after initial_delay (1 second)
        self.assertEqual(
            task.ready_at,
            end_time + datetime.timedelta(seconds=1.0)
        )

    def test_end_execution_with_max_attempts_reached(self):
        """Test that task fails after max attempts"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        # Attempt 1
        task.begin_execution(self.now)
        task.end_execution(self.now + datetime.timedelta(seconds=1), error="Error 1")
        self.assertEqual(task.state, TaskState.ACTIVE)
        
        # Attempt 2
        task.begin_execution(self.now + datetime.timedelta(seconds=2))
        task.end_execution(self.now + datetime.timedelta(seconds=3), error="Error 2")
        self.assertEqual(task.state, TaskState.ACTIVE)
        
        # Attempt 3 (max_attempts reached)
        task.begin_execution(self.now + datetime.timedelta(seconds=5))
        task.end_execution(self.now + datetime.timedelta(seconds=6), error="Error 3")
        self.assertEqual(task.state, TaskState.FAILED)

    def test_end_execution_not_begun(self):
        """Test that ending execution fails if not begun"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        with self.assertRaises(TaskExecutionNotBegunYetError):
            task.end_execution(self.now, error=None)

    def test_last_execution(self):
        """Test last_execution property"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        self.assertIsNone(task.last_execution)
        
        task.begin_execution(self.now)
        self.assertIsNotNone(task.last_execution)
        self.assertEqual(task.last_execution.begun_at, self.now)

    def test_reactivate_failed_task(self):
        """Test reactivating a failed task"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=RetryPolicy(
                initial_delay=1.0,
                backoff_factor=2.0,
                max_delay=60.0,
                max_attempts=2,  # 2 attempts to fail after 2 executions
            ),
            schedule_id=None,
        )
        
        # Fail the task twice to reach max_attempts
        task.begin_execution(self.now)
        task.end_execution(self.now + datetime.timedelta(seconds=1), error="Error 1")
        self.assertEqual(task.state, TaskState.ACTIVE)
        
        task.begin_execution(self.now + datetime.timedelta(seconds=3))
        task.end_execution(self.now + datetime.timedelta(seconds=4), error="Error 2")
        self.assertEqual(task.state, TaskState.FAILED)
        
        # Reactivate it
        reactivate_time = self.now + datetime.timedelta(seconds=10)
        task.reactivate(reactivate_time, delay=5.0)
        
        self.assertEqual(task.state, TaskState.ACTIVE)
        self.assertEqual(
            task.ready_at,
            reactivate_time + datetime.timedelta(seconds=5.0)
        )
        self.assertEqual(task.executions, [])

    def test_reactivate_non_failed_task(self):
        """Test that reactivating non-failed task raises error"""
        task = Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        # Active task
        with self.assertRaises(TaskNotFailedError):
            task.reactivate(self.now, delay=0.0)
        
        # Succeeded task
        task.begin_execution(self.now)
        task.end_execution(self.now + datetime.timedelta(seconds=1), error=None)
        with self.assertRaises(TaskNotFailedError):
            task.reactivate(self.now, delay=0.0)


if __name__ == "__main__":
    unittest.main()

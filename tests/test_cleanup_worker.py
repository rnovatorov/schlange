import unittest
import datetime
from unittest.mock import Mock, call

from schlange.background.cleanup_worker import CleanupWorker
from schlange.core.task import Task
from schlange.core.task_state import TaskState
from schlange.core.retry_policy import RetryPolicy
from schlange.core.cleanup_policy import CleanupPolicy
from schlange.core.errors import TaskNotFoundError


class TestCleanupWorker(unittest.TestCase):
    """Test cases for CleanupWorker"""

    def setUp(self):
        """Set up test fixtures"""
        self.task_service = Mock()
        self.cleanup_policy = CleanupPolicy(
            delete_succeeded_after=3600,  # 1 hour
            delete_failed_after=86400,  # 1 day
        )
        self.now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.retry_policy = RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=60.0,
            max_attempts=3,
        )

    def test_initialization(self):
        """Test CleanupWorker initialization"""
        worker = CleanupWorker(
            interval=60.0,
            task_service=self.task_service,
            cleanup_policy=self.cleanup_policy,
        )
        
        self.assertEqual(worker.interval, 60.0)
        self.assertEqual(worker.task_service, self.task_service)
        self.assertEqual(worker.cleanup_policy, self.cleanup_policy)

    def test_work_with_no_deletable_tasks(self):
        """Test work method when no tasks need deletion"""
        self.task_service.deletable_tasks.return_value = []
        
        worker = CleanupWorker(
            interval=60.0,
            task_service=self.task_service,
            cleanup_policy=self.cleanup_policy,
        )
        
        worker.work()
        
        self.task_service.deletable_tasks.assert_called_once_with(self.cleanup_policy)
        self.task_service.delete_task.assert_not_called()

    def test_work_deletes_tasks(self):
        """Test work method deletes eligible tasks"""
        task1 = Task.create(
            now=self.now - datetime.timedelta(hours=2),
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        task1.begin_execution(self.now - datetime.timedelta(hours=2))
        task1.end_execution(
            self.now - datetime.timedelta(hours=2) + datetime.timedelta(seconds=1),
            error=None,
        )
        
        task2 = Task.create(
            now=self.now - datetime.timedelta(hours=3),
            id="task-2",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        task2.begin_execution(self.now - datetime.timedelta(hours=3))
        task2.end_execution(
            self.now - datetime.timedelta(hours=3) + datetime.timedelta(seconds=1),
            error=None,
        )
        
        self.task_service.deletable_tasks.return_value = [task1, task2]
        
        worker = CleanupWorker(
            interval=60.0,
            task_service=self.task_service,
            cleanup_policy=self.cleanup_policy,
        )
        
        worker.cleanup_tasks()
        
        self.task_service.deletable_tasks.assert_called_once_with(self.cleanup_policy)
        self.assertEqual(self.task_service.delete_task.call_count, 2)
        self.task_service.delete_task.assert_any_call("task-1")
        self.task_service.delete_task.assert_any_call("task-2")

    def test_cleanup_handles_not_found_error(self):
        """Test that TaskNotFoundError is handled gracefully during cleanup"""
        task1 = Task.create(
            now=self.now - datetime.timedelta(hours=2),
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        task1.state = TaskState.SUCCEEDED
        
        task2 = Task.create(
            now=self.now - datetime.timedelta(hours=3),
            id="task-2",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        task2.state = TaskState.SUCCEEDED
        
        self.task_service.deletable_tasks.return_value = [task1, task2]
        
        # First delete succeeds, second raises TaskNotFoundError
        self.task_service.delete_task.side_effect = [None, TaskNotFoundError()]
        
        worker = CleanupWorker(
            interval=60.0,
            task_service=self.task_service,
            cleanup_policy=self.cleanup_policy,
        )
        
        # Should not raise exception
        worker.cleanup_tasks()
        
        # Both deletes should be attempted
        self.assertEqual(self.task_service.delete_task.call_count, 2)

    def test_cleanup_handles_io_error(self):
        """Test that IOError is handled gracefully during cleanup"""
        task = Task.create(
            now=self.now - datetime.timedelta(hours=2),
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        task.state = TaskState.SUCCEEDED
        
        self.task_service.deletable_tasks.return_value = [task]
        self.task_service.delete_task.side_effect = IOError("Database error")
        
        worker = CleanupWorker(
            interval=60.0,
            task_service=self.task_service,
            cleanup_policy=self.cleanup_policy,
        )
        
        # Should not raise exception
        worker.cleanup_tasks()
        
        self.task_service.delete_task.assert_called_once_with("task-1")

    def test_work_calls_cleanup_tasks(self):
        """Test that work method calls cleanup_tasks"""
        self.task_service.deletable_tasks.return_value = []
        
        worker = CleanupWorker(
            interval=60.0,
            task_service=self.task_service,
            cleanup_policy=self.cleanup_policy,
        )
        
        worker.work()
        
        self.task_service.deletable_tasks.assert_called_once()


if __name__ == "__main__":
    unittest.main()

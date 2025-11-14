import unittest
import datetime
from unittest.mock import Mock, MagicMock, call
import time

import schlange


class TestExecutionWorker(unittest.TestCase):
    """Test cases for schlange.background.ExecutionWorker"""

    def setUp(self):
        """Set up test fixtures"""
        self.task_service = Mock()
        self.now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.retry_policy = schlange.RetryPolicy(
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=60.0,
            max_attempts=3,
        )

    def test_initialization(self):
        """Test schlange.background.ExecutionWorker initialization"""
        worker = schlange.background.ExecutionWorker(
            interval=1.0,
            task_service=self.task_service,
            threads=4,
        )
        
        self.assertEqual(worker.interval, 1.0)
        self.assertEqual(worker.task_service, self.task_service)
        self.assertIsNotNone(worker.thread_pool)
        self.assertEqual(len(worker.executing_tasks), 0)

    def test_work_with_no_tasks(self):
        """Test work method when no tasks are available"""
        self.task_service.executable_tasks.return_value = []
        
        worker = schlange.background.ExecutionWorker(
            interval=0.1,
            task_service=self.task_service,
            threads=2,
        )
        
        worker.work()
        
        self.task_service.executable_tasks.assert_called_once()

    def test_work_submits_tasks(self):
        """Test work method submits executable tasks"""
        task1 = schlange.Task.create(
            now=self.now,
            id="task-1",
            args={"key": "value1"},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        task2 = schlange.Task.create(
            now=self.now,
            id="task-2",
            args={"key": "value2"},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        # First call returns tasks, second call returns empty
        self.task_service.executable_tasks.side_effect = [[task1, task2], []]
        
        # Mock execute_task to return completed tasks
        def execute_side_effect(task_id):
            for t in [task1, task2]:
                if t.id == task_id:
                    t.begin_execution(self.now)
                    t.end_execution(self.now + datetime.timedelta(seconds=1), error=None)
                    return t
            raise schlange.TaskNotFoundError()
        
        self.task_service.execute_task.side_effect = execute_side_effect
        
        worker = schlange.background.ExecutionWorker(
            interval=0.1,
            task_service=self.task_service,
            threads=2,
        )
        
        worker.work()
        
        # Give thread pool time to execute
        time.sleep(0.1)
        
        # Verify executable_tasks was called twice (once per iteration)
        self.assertEqual(self.task_service.executable_tasks.call_count, 2)
        
        # Verify tasks were executed
        self.assertEqual(self.task_service.execute_task.call_count, 2)

    def test_submit_task_prevents_duplicate_execution(self):
        """Test that same task cannot be submitted twice simultaneously"""
        task = schlange.Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        # Create a slow-running task execution
        import threading
        execution_started = threading.Event()
        execution_continue = threading.Event()
        
        def slow_execute(task_id):
            execution_started.set()
            execution_continue.wait(timeout=1.0)
            task_copy = schlange.Task.create(
                now=self.now,
                id=task_id,
                args={},
                delay=0.0,
                retry_policy=self.retry_policy,
                schedule_id=None,
            )
            task_copy.begin_execution(self.now)
            task_copy.end_execution(self.now + datetime.timedelta(seconds=1), error=None)
            return task_copy
        
        self.task_service.execute_task.side_effect = slow_execute
        
        worker = schlange.background.ExecutionWorker(
            interval=0.1,
            task_service=self.task_service,
            threads=2,
        )
        
        # First submission should succeed
        result1 = worker._submit_task(task)
        self.assertTrue(result1)
        
        # Wait for execution to start
        execution_started.wait(timeout=1.0)
        
        # schlange.Task should be in executing set
        self.assertIn(task.id, worker.executing_tasks)
        
        # Second submission should fail (task already executing)
        result2 = worker._submit_task(task)
        self.assertFalse(result2)
        
        # Allow execution to complete
        execution_continue.set()
        time.sleep(0.1)

    def test_execute_task_success(self):
        """Test successful task execution"""
        task = schlange.Task.create(
            now=self.now,
            id="task-1",
            args={},
            delay=0.0,
            retry_policy=self.retry_policy,
            schedule_id=None,
        )
        
        # Mock execute_task to return completed task
        task.begin_execution(self.now)
        task.end_execution(self.now + datetime.timedelta(seconds=1), error=None)
        self.task_service.execute_task.return_value = task
        
        worker = schlange.background.ExecutionWorker(
            interval=0.1,
            task_service=self.task_service,
            threads=2,
        )
        
        worker._execute_task("task-1")
        
        self.task_service.execute_task.assert_called_once_with("task-1")

    def test_execute_task_handles_not_found_error(self):
        """Test that schlange.TaskNotFoundError is handled gracefully"""
        self.task_service.execute_task.side_effect = schlange.TaskNotFoundError()
        
        worker = schlange.background.ExecutionWorker(
            interval=0.1,
            task_service=self.task_service,
            threads=2,
        )
        
        # Should not raise exception
        worker._execute_task("task-1")
        
        self.task_service.execute_task.assert_called_once_with("task-1")

    def test_execute_task_handles_not_active_error(self):
        """Test that schlange.TaskNotActiveError is handled gracefully"""
        self.task_service.execute_task.side_effect = schlange.TaskNotActiveError()
        
        worker = schlange.background.ExecutionWorker(
            interval=0.1,
            task_service=self.task_service,
            threads=2,
        )
        
        # Should not raise exception
        worker._execute_task("task-1")
        
        self.task_service.execute_task.assert_called_once_with("task-1")

    def test_execute_task_handles_handler_not_found(self):
        """Test that schlange.TaskHandlerNotFound is handled gracefully"""
        self.task_service.execute_task.side_effect = schlange.TaskHandlerNotFound()
        
        worker = schlange.background.ExecutionWorker(
            interval=0.1,
            task_service=self.task_service,
            threads=2,
        )
        
        # Should not raise exception
        worker._execute_task("task-1")
        
        self.task_service.execute_task.assert_called_once_with("task-1")

    def test_stop_shuts_down_thread_pool(self):
        """Test that stop method shuts down the thread pool"""
        worker = schlange.background.ExecutionWorker(
            interval=0.1,
            task_service=self.task_service,
            threads=2,
        )
        
        worker.start()
        time.sleep(0.05)
        worker.stop()
        
        # Verify worker stopped
        self.assertTrue(worker.stopped.is_set())
        
        # Verify thread pool was shut down
        self.assertTrue(worker.thread_pool._shutdown)


if __name__ == "__main__":
    unittest.main()

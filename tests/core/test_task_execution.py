import unittest
import datetime

import schlange


class TestTaskExecution(unittest.TestCase):
    """Test cases for schlange.TaskExecution"""

    def test_begin(self):
        """Test beginning a task execution"""
        timestamp = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        execution = schlange.TaskExecution.begin(timestamp)
        
        self.assertEqual(execution.begun_at, timestamp)
        self.assertIsNone(execution.ended_at)
        self.assertIsNone(execution.error)
        self.assertFalse(execution.ended)

    def test_end_without_error(self):
        """Test ending a task execution successfully"""
        begin_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        end_time = datetime.datetime(2025, 1, 1, 12, 0, 5, tzinfo=datetime.UTC)
        
        execution = schlange.TaskExecution.begin(begin_time)
        execution.end(end_time, error=None)
        
        self.assertEqual(execution.ended_at, end_time)
        self.assertIsNone(execution.error)
        self.assertTrue(execution.ended)

    def test_end_with_error(self):
        """Test ending a task execution with error"""
        begin_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        end_time = datetime.datetime(2025, 1, 1, 12, 0, 5, tzinfo=datetime.UTC)
        error_msg = "Something went wrong"
        
        execution = schlange.TaskExecution.begin(begin_time)
        execution.end(end_time, error=error_msg)
        
        self.assertEqual(execution.ended_at, end_time)
        self.assertEqual(execution.error, error_msg)
        self.assertTrue(execution.ended)

    def test_duration(self):
        """Test duration calculation"""
        begin_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        end_time = datetime.datetime(2025, 1, 1, 12, 0, 10, tzinfo=datetime.UTC)
        
        execution = schlange.TaskExecution.begin(begin_time)
        self.assertIsNone(execution.duration)
        
        execution.end(end_time, error=None)
        self.assertEqual(execution.duration, datetime.timedelta(seconds=10))


if __name__ == "__main__":
    unittest.main()

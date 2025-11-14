import unittest
import datetime

import schlange


class TestScheduleFiring(unittest.TestCase):
    """Test cases for schlange.ScheduleFiring"""

    def test_begin(self):
        """Test beginning a schedule firing"""
        timestamp = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        firing = schlange.ScheduleFiring.begin(timestamp, task_sequence_number=1)
        
        self.assertEqual(firing.task_sequence_number, 1)
        self.assertEqual(firing.begun_at, timestamp)
        self.assertIsNone(firing.ended_at)
        self.assertIsNone(firing.error)
        self.assertFalse(firing.ended)

    def test_end_without_error(self):
        """Test ending a schedule firing successfully"""
        begin_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        end_time = datetime.datetime(2025, 1, 1, 12, 0, 5, tzinfo=datetime.UTC)
        
        firing = schlange.ScheduleFiring.begin(begin_time, task_sequence_number=1)
        firing.end(end_time, error=None)
        
        self.assertEqual(firing.ended_at, end_time)
        self.assertIsNone(firing.error)
        self.assertTrue(firing.ended)

    def test_end_with_error(self):
        """Test ending a schedule firing with error"""
        begin_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        end_time = datetime.datetime(2025, 1, 1, 12, 0, 5, tzinfo=datetime.UTC)
        error_msg = "Failed to create task"
        
        firing = schlange.ScheduleFiring.begin(begin_time, task_sequence_number=2)
        firing.end(end_time, error=error_msg)
        
        self.assertEqual(firing.ended_at, end_time)
        self.assertEqual(firing.error, error_msg)
        self.assertTrue(firing.ended)

    def test_duration(self):
        """Test duration calculation"""
        begin_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        end_time = datetime.datetime(2025, 1, 1, 12, 0, 10, tzinfo=datetime.UTC)
        
        firing = schlange.ScheduleFiring.begin(begin_time, task_sequence_number=1)
        self.assertIsNone(firing.duration)
        
        firing.end(end_time, error=None)
        self.assertEqual(firing.duration, datetime.timedelta(seconds=10))


if __name__ == "__main__":
    unittest.main()

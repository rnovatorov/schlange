import unittest
import time
import threading

import schlange


class TestWorker(unittest.TestCase):
    """Test cases for schlange.background.Worker base class"""

    def test_worker_initialization(self):
        """Test worker initializes with correct attributes"""
        
        class TestWorkerImpl(schlange.background.Worker):
            def work(self):
                pass
        
        worker = TestWorkerImpl(name="test-worker", interval=0.1)
        
        self.assertEqual(worker.name, "test-worker")
        self.assertEqual(worker.interval, 0.1)
        self.assertFalse(worker.stopping.is_set())
        self.assertFalse(worker.stopped.is_set())
        self.assertIsInstance(worker.stopping, threading.Event)
        self.assertIsInstance(worker.stopped, threading.Event)

    def test_worker_context_manager(self):
        """Test worker can be used as context manager"""
        
        work_called = []
        stop_after = 2
        
        class TestWorkerImpl(schlange.background.Worker):
            def work(self):
                work_called.append(1)
                if len(work_called) >= stop_after:
                    self.stopping.set()
        
        with TestWorkerImpl(name="test-worker", interval=0.01) as worker:
            # schlange.background.Worker should start automatically
            self.assertTrue(worker.is_alive())
            # Wait a bit for work to be called
            time.sleep(0.05)
        
        # schlange.background.Worker should stop when exiting context
        self.assertTrue(worker.stopped.is_set())
        self.assertFalse(worker.is_alive())
        # Verify work was called at least once
        self.assertGreater(len(work_called), 0)

    def test_worker_stop(self):
        """Test worker stops correctly"""
        
        work_count = []
        
        class TestWorkerImpl(schlange.background.Worker):
            def work(self):
                work_count.append(1)
        
        worker = TestWorkerImpl(name="test-worker", interval=0.01)
        worker.start()
        
        # Let it work a few times
        time.sleep(0.05)
        
        # Stop the worker
        worker.stop()
        
        # Verify it stopped
        self.assertTrue(worker.stopped.is_set())
        self.assertTrue(worker.stopping.is_set())
        self.assertFalse(worker.is_alive())
        
        # Verify work was called
        self.assertGreater(len(work_count), 0)

    def test_worker_loop_respects_interval(self):
        """Test worker loop respects the interval"""
        
        work_times = []
        
        class TestWorkerImpl(schlange.background.Worker):
            def work(self):
                work_times.append(time.time())
                if len(work_times) >= 3:
                    self.stopping.set()
        
        worker = TestWorkerImpl(name="test-worker", interval=0.05)
        worker.start()
        worker.stopped.wait(timeout=1.0)
        
        # Verify we got at least 3 work calls
        self.assertGreaterEqual(len(work_times), 3)
        
        # Verify there's roughly the interval between calls
        if len(work_times) >= 2:
            interval = work_times[1] - work_times[0]
            # Allow some tolerance (between 0.03 and 0.2 seconds)
            self.assertGreater(interval, 0.03)
            self.assertLess(interval, 0.2)

    def test_worker_work_not_implemented(self):
        """Test that schlange.background.Worker.work() raises NotImplementedError"""
        
        worker = schlange.background.Worker(name="test-worker", interval=0.1)
        
        with self.assertRaises(NotImplementedError):
            worker.work()

    def test_worker_multiple_stop_calls(self):
        """Test that multiple stop calls don't cause issues"""
        
        class TestWorkerImpl(schlange.background.Worker):
            def work(self):
                pass
        
        worker = TestWorkerImpl(name="test-worker", interval=0.1)
        worker.start()
        
        # Stop multiple times
        worker.stop()
        worker.stop()
        worker.stop()
        
        # Should be stopped
        self.assertTrue(worker.stopped.is_set())
        self.assertFalse(worker.is_alive())


if __name__ == "__main__":
    unittest.main()

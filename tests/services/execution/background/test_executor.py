import threading
import time
import unittest
from unittest import mock

from schlange.services.execution.background.executor import Executor


def _occupy(started, release):
    started.set()
    release.wait(5)


def _make():
    e = Executor(interval=1.0, task_service=mock.Mock(), threads=2)
    e.thread_pool = mock.Mock()
    return e


class ExecutorPoolLifecycleContractTest(unittest.TestCase):
    """cancel()/wait() drive the pool with the right shutdown arguments."""

    def test_cancel_shuts_down_pool_non_blocking_with_cancel_futures(self):
        e = _make()
        e.cancel()
        e.thread_pool.shutdown.assert_called_once_with(wait=False, cancel_futures=True)

    def test_wait_drains_pool_blocking(self):
        e = _make()
        e.stopped.set()
        e.wait()
        e.thread_pool.shutdown.assert_called_once_with(wait=True)

    def test_cancel_then_wait_splits_pool_lifecycle_in_order(self):
        e = _make()
        e.cancel()
        e.stopped.set()
        e.wait()
        self.assertEqual(
            [c.kwargs for c in e.thread_pool.shutdown.call_args_list],
            [{"wait": False, "cancel_futures": True}, {"wait": True}],
        )


class ExecutorPoolLifecycleBehaviorTest(unittest.TestCase):
    """Exercise the split against a real ThreadPoolExecutor."""

    def _executor(self, threads=1):
        return Executor(interval=1.0, task_service=mock.Mock(), threads=threads)

    def test_cancel_cancels_queued_futures_without_blocking(self):
        e = self._executor(threads=1)
        started, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        e.thread_pool.submit(_occupy, started, release)
        started.wait(2)  # the single worker is now busy in-flight
        queued = e.thread_pool.submit(lambda: None)  # pending in the queue

        e.cancel()

        self.assertTrue(queued.cancelled())
        release.set()
        e.stopped.set()
        e.wait()

    def test_wait_blocks_until_in_flight_work_finishes(self):
        e = self._executor()
        finished = threading.Event()
        e.thread_pool.submit(lambda: (time.sleep(0.3), finished.set())[1])

        e.cancel()  # non-blocking; in-flight keeps running
        self.assertFalse(finished.is_set())

        e.stopped.set()
        t0 = time.time()
        e.wait()
        self.assertGreater(time.time() - t0, 0.2)  # blocked to drain the pool
        self.assertTrue(finished.is_set())


if __name__ == "__main__":
    unittest.main()

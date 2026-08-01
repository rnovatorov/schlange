import unittest
from unittest import mock

from schlange.schlange import Schlange


def _make_schlange(**overrides):
    workers = {
        "cleanup_worker": mock.Mock(),
        "dispatcher": mock.Mock(),
        "executor": mock.Mock(),
        "schedule_worker": mock.Mock(),
        "leases_reaper": mock.Mock(),
    }
    workers.update(overrides)
    s = Schlange(
        task_service=None,
        default_retry_policy=None,
        schedule_service=None,
        **workers,
    )
    return s, workers.values()


class SchlangeStopTest(unittest.TestCase):

    def test_stop_cancels_and_waits_every_worker(self):
        s, workers = _make_schlange()
        s.stop()
        for w in workers:
            w.cancel.assert_called_once()
            w.wait.assert_called_once()

    def test_stop_does_not_bail_when_a_worker_wait_raises(self):
        boom = RuntimeError("executor boom")
        executor = mock.Mock()
        executor.wait.side_effect = boom
        s, workers = _make_schlange(executor=executor)
        with self.assertRaises(ExceptionGroup) as ctx:
            s.stop()
        # every worker was still cancelled and waited despite the raise
        for w in workers:
            w.cancel.assert_called_once()
            w.wait.assert_called_once()
        # the failed worker's error surfaced aggregated, not swallowed
        self.assertEqual(list(ctx.exception.exceptions), [boom])

    def test_stop_aggregates_multiple_failures(self):
        first = RuntimeError("first")
        second = RuntimeError("second")
        dispatcher = mock.Mock()
        dispatcher.wait.side_effect = first
        reaper = mock.Mock()
        reaper.wait.side_effect = second
        s, _ = _make_schlange(dispatcher=dispatcher, leases_reaper=reaper)
        with self.assertRaises(ExceptionGroup) as ctx:
            s.stop()
        self.assertEqual(set(ctx.exception.exceptions), {first, second})


if __name__ == "__main__":
    unittest.main()

import datetime
import unittest

from schlange.internal import core as internal_core
from schlange.services.tasks import core


def _retry_policy():
    return internal_core.RetryPolicy(
        initial_delay=1,
        backoff_factor=2,
        max_delay=None,
        max_attempts=3,
    )


def _immediate_retry_policy():
    return internal_core.RetryPolicy(
        initial_delay=0,
        backoff_factor=2,
        max_delay=None,
        max_attempts=3,
    )


def _now():
    return datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)


def _create_task(**overrides):
    defaults = dict(
        now=_now(),
        id="task-1",
        kind="test_kind",
        args={"key": "value"},
        delay=0,
        retry_policy=_retry_policy(),
        visibility_timeout=30.0,
        schedule_id=None,
    )
    defaults.update(overrides)
    return core.Task.create(**defaults)


class TaskBeginExecutionTest(unittest.TestCase):

    def test_begin_execution_generates_seq_num_zero_first(self):
        task = _create_task()
        task.begin_execution(now=_now())
        self.assertEqual(task.executions[0].seq_num, 0)

    def test_begin_execution_increments_seq_num(self):
        task = _create_task(retry_policy=_immediate_retry_policy())
        for expected in (0, 1, 2):
            task.begin_execution(now=_now())
            self.assertEqual(task.last_execution.seq_num, expected)
            task.end_execution(
                seq_num=task.last_execution.seq_num,
                now=_now(),
                error="boom",
            )

    def test_begin_execution_raises_when_outstanding_not_ended(self):
        task = _create_task()
        task.begin_execution(now=_now())
        with self.assertRaises(core.TaskExecutionNotEndedYetError):
            task.begin_execution(now=_now())

    def test_begin_execution_works_after_last_execution_ended(self):
        task = _create_task(retry_policy=_immediate_retry_policy())
        task.begin_execution(now=_now())
        task.end_execution(seq_num=0, now=_now(), error="boom")
        task.begin_execution(now=_now())
        self.assertEqual(task.last_execution.seq_num, 1)
        self.assertFalse(task.last_execution.ended)

    def test_begin_execution_raises_when_not_active(self):
        task = _create_task()
        task.begin_execution(now=_now())
        task.end_execution(seq_num=0, now=_now(), error=None)
        with self.assertRaises(core.TaskNotActiveError):
            task.begin_execution(now=_now())

    def test_begin_execution_raises_when_not_ready(self):
        task = _create_task(delay=3600)
        with self.assertRaises(core.TaskNotReadyError):
            task.begin_execution(now=_now())


class TaskEndExecutionTest(unittest.TestCase):

    def test_end_execution_finds_right_execution_by_seq_num(self):
        task = _create_task(retry_policy=_immediate_retry_policy())
        task.begin_execution(now=_now())
        task.end_execution(seq_num=0, now=_now(), error="boom")
        task.begin_execution(now=_now())
        self.assertFalse(task.last_execution.ended)
        task.end_execution(seq_num=1, now=_now(), error=None)
        self.assertTrue(task.get_execution(1).ended)
        self.assertEqual(task.state, core.TaskState.SUCCEEDED)

    def test_end_execution_is_idempotent_when_already_ended(self):
        task = _create_task()
        task.begin_execution(now=_now())
        task.end_execution(seq_num=0, now=_now(), error=None)
        self.assertEqual(task.state, core.TaskState.SUCCEEDED)
        task.end_execution(seq_num=0, now=_now(), error="late")
        self.assertEqual(task.state, core.TaskState.SUCCEEDED)

    def test_end_execution_success_sets_succeeded(self):
        task = _create_task()
        task.begin_execution(now=_now())
        task.end_execution(seq_num=0, now=_now(), error=None)
        self.assertEqual(task.state, core.TaskState.SUCCEEDED)

    def test_end_execution_failure_retries_when_attempts_remain(self):
        task = _create_task()
        before = task.ready_at
        task.begin_execution(now=_now())
        task.end_execution(seq_num=0, now=_now(), error="boom")
        self.assertEqual(task.state, core.TaskState.ACTIVE)
        self.assertGreater(task.ready_at, before)

    def test_end_execution_failure_marks_failed_at_max_attempts(self):
        retry_policy = internal_core.RetryPolicy(
            initial_delay=0,
            backoff_factor=2,
            max_delay=None,
            max_attempts=3,
        )
        task = _create_task(retry_policy=retry_policy)
        for seq in range(3):
            task.begin_execution(now=_now())
            task.end_execution(seq_num=seq, now=_now(), error="boom")
        self.assertEqual(task.state, core.TaskState.FAILED)

    def test_end_execution_unknown_seq_num_raises(self):
        task = _create_task()
        task.begin_execution(now=_now())
        with self.assertRaises(core.TaskExecutionNotFoundError):
            task.end_execution(seq_num=999, now=_now(), error=None)


class TaskGetExecutionTest(unittest.TestCase):

    def test_get_execution_returns_execution_by_seq_num(self):
        task = _create_task()
        task.begin_execution(now=_now())
        execution = task.get_execution(0)
        self.assertEqual(execution.seq_num, 0)
        self.assertEqual(execution, task.last_execution)

    def test_get_execution_unknown_seq_num_raises(self):
        task = _create_task()
        with self.assertRaises(core.TaskExecutionNotFoundError):
            task.get_execution(0)


if __name__ == "__main__":
    unittest.main()

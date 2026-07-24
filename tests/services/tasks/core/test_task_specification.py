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


def _now():
    return datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)


def _create_task(**overrides):
    defaults = dict(
        now=_now(),
        id="task-1",
        kind="test_kind",
        args={},
        delay=0,
        retry_policy=_retry_policy(),
        schedule_id=None,
    )
    defaults.update(overrides)
    return core.Task.create(**defaults)


class TaskSpecificationExecutionInProgressTest(unittest.TestCase):

    def test_execution_in_progress_true_matches_task_with_outstanding_execution(self):
        task = _create_task()
        task.begin_execution(now=_now())
        spec = core.TaskSpecification(execution_in_progress=True)
        self.assertTrue(spec.is_satisfied_by(task))

    def test_execution_in_progress_true_does_not_match_no_executions(self):
        task = _create_task()
        spec = core.TaskSpecification(execution_in_progress=True)
        self.assertFalse(spec.is_satisfied_by(task))

    def test_execution_in_progress_true_does_not_match_ended_execution(self):
        task = _create_task()
        task.begin_execution(now=_now())
        task.end_execution(seq_num=0, now=_now(), error="boom")
        spec = core.TaskSpecification(execution_in_progress=True)
        self.assertFalse(spec.is_satisfied_by(task))

    def test_execution_in_progress_false_matches_no_executions(self):
        task = _create_task()
        spec = core.TaskSpecification(execution_in_progress=False)
        self.assertTrue(spec.is_satisfied_by(task))

    def test_execution_in_progress_false_matches_ended_execution(self):
        task = _create_task()
        task.begin_execution(now=_now())
        task.end_execution(seq_num=0, now=_now(), error=None)
        spec = core.TaskSpecification(execution_in_progress=False)
        self.assertTrue(spec.is_satisfied_by(task))

    def test_execution_in_progress_false_does_not_match_outstanding_execution(self):
        task = _create_task()
        task.begin_execution(now=_now())
        spec = core.TaskSpecification(execution_in_progress=False)
        self.assertFalse(spec.is_satisfied_by(task))

    def test_execution_in_progress_none_matches_all_tasks(self):
        no_exec = _create_task(id="a")
        begun = _create_task(id="b")
        begun.begin_execution(now=_now())
        ended = _create_task(id="c")
        ended.begin_execution(now=_now())
        ended.end_execution(seq_num=0, now=_now(), error=None)
        spec = core.TaskSpecification(execution_in_progress=None)
        self.assertTrue(spec.is_satisfied_by(no_exec))
        self.assertTrue(spec.is_satisfied_by(begun))
        self.assertTrue(spec.is_satisfied_by(ended))

    def test_execution_in_progress_combines_with_other_filters(self):
        task = _create_task()
        task.begin_execution(now=_now())
        spec = core.TaskSpecification(
            state=core.TaskState.ACTIVE,
            ready_as_of=_now(),
            execution_in_progress=True,
        )
        self.assertTrue(spec.is_satisfied_by(task))
        spec = core.TaskSpecification(
            state=core.TaskState.ACTIVE,
            ready_as_of=_now(),
            execution_in_progress=False,
        )
        self.assertFalse(spec.is_satisfied_by(task))


if __name__ == "__main__":
    unittest.main()

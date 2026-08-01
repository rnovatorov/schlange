import unittest
from unittest import mock

from schlange.services.execution.core import (
    AbortedError,
    Error,
    ExecutionService,
    FailedPreconditionError,
    NotFoundError,
    TaskExecution,
)


def _make_service(handlers, task_service=None):
    if task_service is None:
        task_service = mock.Mock(spec=["end_execution"])
        task_service.end_execution.return_value = None
    return ExecutionService(handlers=handlers, task_service=task_service)


class ExecuteTaskTest(unittest.TestCase):
    def test_execute_with_successful_handler_calls_end_execution_no_error(self):
        handler = mock.Mock(spec=["__call__"])
        service = _make_service(handlers={"test_kind": handler})

        service.execute(task_id="task-1", seq_num=0, kind="test_kind", args={"a": 1})

        handler.assert_called_once()
        service.task_service.end_execution.assert_called_once_with("task-1", 0, None)

    def test_execute_with_handler_that_raises_records_error_string(self):
        handler = mock.Mock(spec=["__call__"], side_effect=ValueError("boom"))
        service = _make_service(handlers={"test_kind": handler})

        service.execute(task_id="task-1", seq_num=2, kind="test_kind", args={"a": 1})

        handler.assert_called_once()
        service.task_service.end_execution.assert_called_once_with("task-1", 2, "boom")

    def test_execute_with_unregistered_kind_raises_not_found_error(self):
        handler = mock.Mock(spec=["__call__"])
        task_service = mock.Mock(spec=["end_execution"])
        service = ExecutionService(
            handlers={"test_kind": handler}, task_service=task_service
        )

        with self.assertRaises(NotFoundError):
            service.execute(task_id="task-1", seq_num=0, kind="unknown_kind", args={})

        handler.assert_not_called()
        task_service.end_execution.assert_not_called()

    def test_execute_when_end_execution_raises_aborted_error_propagates(self):
        handler = mock.Mock(spec=["__call__"])
        task_service = mock.Mock(spec=["end_execution"])
        task_service.end_execution.side_effect = AbortedError("conflict")
        service = ExecutionService(
            handlers={"test_kind": handler}, task_service=task_service
        )

        with self.assertRaises(AbortedError):
            service.execute(task_id="task-1", seq_num=0, kind="test_kind", args={})

    def test_execute_when_end_execution_raises_not_found_error_propagates(self):
        handler = mock.Mock(spec=["__call__"])
        task_service = mock.Mock(spec=["end_execution"])
        task_service.end_execution.side_effect = NotFoundError("missing")
        service = ExecutionService(
            handlers={"test_kind": handler}, task_service=task_service
        )

        with self.assertRaises(NotFoundError):
            service.execute(task_id="task-1", seq_num=0, kind="test_kind", args={})

    def test_execute_when_end_execution_raises_failed_precondition_propagates(
        self,
    ):
        handler = mock.Mock(spec=["__call__"])
        task_service = mock.Mock(spec=["end_execution"])
        task_service.end_execution.side_effect = FailedPreconditionError("wrong state")
        service = ExecutionService(
            handlers={"test_kind": handler}, task_service=task_service
        )

        with self.assertRaises(FailedPreconditionError):
            service.execute(task_id="task-1", seq_num=0, kind="test_kind", args={})

    def test_handler_receives_correct_task_execution_data(self):
        received: list[TaskExecution] = []

        def capture(execution: TaskExecution) -> None:
            received.append(execution)

        service = _make_service(handlers={"test_kind": capture})

        service.execute(
            task_id="task-7",
            seq_num=3,
            kind="test_kind",
            args={"x": 1, "y": 2},
        )

        self.assertEqual(len(received), 1)
        execution = received[0]
        self.assertEqual(execution.task_id, "task-7")
        self.assertEqual(execution.seq_num, 3)
        self.assertEqual(execution.args, {"x": 1, "y": 2})


class ErrorsTest(unittest.TestCase):
    def test_all_errors_subclass_error(self):
        for error_cls in (AbortedError, NotFoundError, FailedPreconditionError):
            self.assertTrue(issubclass(error_cls, Error))
            self.assertTrue(issubclass(error_cls, Exception))


if __name__ == "__main__":
    unittest.main()

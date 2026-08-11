import unittest
from unittest import mock

from schlange.api import tasks
from schlange.internal import core
from schlange.services.schedules.api import TaskServiceAdapter


def _retry_policy():
    return core.RetryPolicy(
        initial_delay=1, backoff_factor=2, max_delay=None, max_attempts=3
    )


def _call_adapter(adapter):
    adapter.create_task(
        id="task-1",
        args={"k": "v"},
        kind="test",
        delay=0,
        visibility_timeout=30.0,
        retry_policy=_retry_policy(),
        schedule_id="sched-1",
    )


class TaskServiceAdapterTest(unittest.TestCase):
    def test_create_task_builds_request_and_calls_server(self):
        task_server = mock.Mock(spec=["create_task"])
        adapter = TaskServiceAdapter(task_server=task_server)

        _call_adapter(adapter)

        task_server.create_task.assert_called_once()
        request = task_server.create_task.call_args.args[0]
        self.assertIsInstance(request, tasks.CreateTaskRequest)
        self.assertEqual(request.id, "task-1")
        self.assertEqual(request.args, {"k": "v"})
        self.assertEqual(request.kind, "test")
        self.assertEqual(request.delay, 0)
        self.assertEqual(request.visibility_timeout, 30.0)
        self.assertEqual(request.schedule_id, "sched-1")
        self.assertIsInstance(request.retry_policy, tasks.RetryPolicy)
        self.assertEqual(request.retry_policy.initial_delay, 1)
        self.assertEqual(request.retry_policy.backoff_factor, 2)
        self.assertIsNone(request.retry_policy.max_delay)
        self.assertEqual(request.retry_policy.max_attempts, 3)

    def test_create_task_swallows_already_exists(self):
        task_server = mock.Mock(spec=["create_task"])
        task_server.create_task.side_effect = tasks.AlreadyExistsError()
        adapter = TaskServiceAdapter(task_server=task_server)

        _call_adapter(adapter)  # must not raise

        task_server.create_task.assert_called_once()

    def test_create_task_propagates_unexpected_errors(self):
        task_server = mock.Mock(spec=["create_task"])
        task_server.create_task.side_effect = IOError("boom")
        adapter = TaskServiceAdapter(task_server=task_server)

        with self.assertRaises(IOError):
            _call_adapter(adapter)


if __name__ == "__main__":
    unittest.main()

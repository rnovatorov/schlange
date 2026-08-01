import unittest
from unittest import mock

from schlange.services.tasks import background as tasks_background
from schlange.services.tasks import core as tasks_core


def _task(task_id):
    import datetime

    from schlange.internal import core as internal_core

    return tasks_core.Task.create(
        now=datetime.datetime.now(datetime.UTC),
        id=task_id,
        kind="test_kind",
        args={},
        delay=0,
        retry_policy=internal_core.RetryPolicy(
            initial_delay=1, backoff_factor=2, max_delay=None, max_attempts=3
        ),
        visibility_timeout=30.0,
        schedule_id=None,
    )


class DispatcherWorkTest(unittest.TestCase):
    def test_work_noops_when_acquire_fails(self):
        service = mock.MagicMock()
        service.acquire_lease.return_value = False
        service.executable_tasks.return_value = [_task("t1"), _task("t2")]
        dispatcher = tasks_background.Dispatcher(
            service=service, holder="h", key="k", ttl=5.0, interval=1.0
        )
        dispatcher.work()
        service.begin_execution.assert_not_called()

    def test_work_dispatches_tasks_when_acquire_succeeds(self):
        service = mock.MagicMock()
        service.acquire_lease.return_value = True
        service.executable_tasks.return_value = [_task("t1"), _task("t2")]
        dispatcher = tasks_background.Dispatcher(
            service=service, holder="h", key="k", ttl=5.0, interval=1.0
        )
        dispatcher.work()
        self.assertEqual(service.begin_execution.call_count, 2)

    def test_work_continues_after_io_error(self):
        service = mock.MagicMock()
        service.acquire_lease.return_value = True
        service.executable_tasks.return_value = [_task("t1"), _task("t2")]
        service.begin_execution.side_effect = [IOError("boom"), None]
        dispatcher = tasks_background.Dispatcher(
            service=service, holder="h", key="k", ttl=5.0, interval=1.0
        )
        dispatcher.work()
        self.assertEqual(service.begin_execution.call_count, 2)

    def test_work_continues_after_domain_error(self):
        service = mock.MagicMock()
        service.acquire_lease.return_value = True
        service.executable_tasks.return_value = [_task("t1"), _task("t2")]
        service.begin_execution.side_effect = [tasks_core.TaskNotActiveError(), None]
        dispatcher = tasks_background.Dispatcher(
            service=service, holder="h", key="k", ttl=5.0, interval=1.0
        )
        dispatcher.work()
        self.assertEqual(service.begin_execution.call_count, 2)

    def test_work_acquires_lease_with_correct_key_holder_ttl(self):
        service = mock.MagicMock()
        service.acquire_lease.return_value = False
        service.executable_tasks.return_value = []
        dispatcher = tasks_background.Dispatcher(
            service=service, holder="h1", key="k1", ttl=7.0, interval=1.0
        )
        dispatcher.work()
        service.acquire_lease.assert_called_once_with("k1", "h1", 7.0)


if __name__ == "__main__":
    unittest.main()

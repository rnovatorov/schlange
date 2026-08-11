import unittest
from unittest import mock

from schlange.services.schedules import background as schedules_background
from schlange.services.schedules import core as schedules_core


def _schedule(schedule_id):
    schedule = mock.Mock()
    schedule.id = schedule_id
    return schedule


class ScheduleWorkerWorkTest(unittest.TestCase):
    def test_work_noops_when_acquire_fails(self):
        service = mock.MagicMock()
        service.acquire_lease.return_value = False
        service.fireable_schedules.return_value = [_schedule("s1"), _schedule("s2")]
        worker = schedules_background.ScheduleWorker(
            schedule_service=service, holder="h", key="k", ttl=5.0, interval=1.0
        )

        worker.work()

        service.fire_schedule.assert_not_called()

    def test_work_fires_schedules_when_acquire_succeeds(self):
        service = mock.MagicMock()
        service.acquire_lease.return_value = True
        service.fireable_schedules.return_value = [_schedule("s1"), _schedule("s2")]
        worker = schedules_background.ScheduleWorker(
            schedule_service=service, holder="h", key="k", ttl=5.0, interval=1.0
        )

        worker.work()

        self.assertEqual(service.fire_schedule.call_count, 2)

    def test_work_continues_after_io_error(self):
        service = mock.MagicMock()
        service.acquire_lease.return_value = True
        service.fireable_schedules.return_value = [_schedule("s1"), _schedule("s2")]
        service.fire_schedule.side_effect = [IOError("boom"), mock.MagicMock()]
        worker = schedules_background.ScheduleWorker(
            schedule_service=service, holder="h", key="k", ttl=5.0, interval=1.0
        )

        worker.work()

        self.assertEqual(service.fire_schedule.call_count, 2)

    def test_work_continues_after_domain_error(self):
        service = mock.MagicMock()
        service.acquire_lease.return_value = True
        service.fireable_schedules.return_value = [_schedule("s1"), _schedule("s2")]
        service.fire_schedule.side_effect = [
            schedules_core.ScheduleNotFoundError(),
            mock.MagicMock(),
        ]
        worker = schedules_background.ScheduleWorker(
            schedule_service=service, holder="h", key="k", ttl=5.0, interval=1.0
        )

        worker.work()

        self.assertEqual(service.fire_schedule.call_count, 2)

    def test_work_acquires_lease_with_correct_key_holder_ttl(self):
        service = mock.MagicMock()
        service.acquire_lease.return_value = False
        service.fireable_schedules.return_value = []
        worker = schedules_background.ScheduleWorker(
            schedule_service=service, holder="h1", key="k1", ttl=7.0, interval=1.0
        )

        worker.work()

        service.acquire_lease.assert_called_once_with("k1", "h1", 7.0)


if __name__ == "__main__":
    unittest.main()

import unittest

from schlange.services.lease_manager.background.reaper import Reaper


class FakeService:
    def __init__(self) -> None:
        self.delete_expired_calls = 0

    def delete_expired(self) -> int:
        self.delete_expired_calls += 1
        return 0


class ReaperTest(unittest.TestCase):

    def test_work_calls_delete_expired(self):
        service = FakeService()
        reaper = Reaper(service=service, interval=1.0)
        reaper.work()
        self.assertEqual(service.delete_expired_calls, 1)


if __name__ == "__main__":
    unittest.main()

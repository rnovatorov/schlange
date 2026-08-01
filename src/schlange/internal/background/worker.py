import os
import signal
import threading


class Worker(threading.Thread):

    def __init__(self, name: str, interval: float) -> None:
        threading.Thread.__init__(self, name=name)
        self.interval = interval
        self.error: Exception | None = None
        self.stopping = threading.Event()
        self.stopped = threading.Event()

    def __enter__(self) -> "Worker":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def stop(self) -> None:
        self.cancel()
        self.wait()

    def cancel(self) -> None:
        self.stopping.set()

    def wait(self) -> None:
        self.stopped.wait()
        if self.error is not None:
            raise self.error

    def run(self) -> None:
        try:
            self.loop()
        except Exception as e:
            self.error = e
            os.kill(os.getpid(), signal.SIGINT)
        finally:
            self.stopped.set()

    def loop(self) -> None:
        while not self.stopping.is_set():
            self.work()
            self.stopping.wait(self.interval)

    def work(self) -> None:
        raise NotImplementedError

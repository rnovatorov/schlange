import threading


class Worker(threading.Thread):

    def __init__(self, name: str, interval: float) -> None:
        threading.Thread.__init__(self, name=name)
        self.interval = interval
        self.stopping = threading.Event()
        self.stopped = threading.Event()

    def __enter__(self) -> "Worker":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def stop(self) -> None:
        self.stopping.set()
        self.stopped.wait()

    def run(self) -> None:
        try:
            self.loop()
        finally:
            self.stopped.set()

    def loop(self) -> None:
        while not self.stopping.is_set():
            self.work()
            self.stopping.wait(self.interval)

    def work(self) -> None:
        raise NotImplementedError

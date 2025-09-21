# Schlange

[![CI](https://github.com/rnovatorov/schlange/actions/workflows/ci.yml/badge.svg)](https://github.com/rnovatorov/schlange/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/schlange-queue.svg)](https://pypi.org/project/schlange-queue)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black)

**Schlange** is a lightweight, persistent, single-node task queue and scheduler
built on top of SQLite.

It's designed for simplicity and reliability and might be a good choice when
you need background jobs without complexities of RabbitMQ, Redis or Kafka.

## Features

- **Persistence** - rely on rock-solid SQLite database for durable storage.
- **Scheduler** - create persistent interval-based schedules for recurring tasks.
- **Concurrency** - execute tasks concurrently using a thread pool.
- **Retries** - configure exponential backoff and max attempts.
- **Cleanup** - automatically delete old tasks.
- **CLI** - manage tasks and schedules from the command line.

## Quick Start

Install from PyPI:

```bash
pip install schlange-queue
```

Checkout [examples](examples).

## Alternatives

- [Celery](https://github.com/celery/celery). Battle-tested, heavyweight.
- [APScheduler](https://github.com/agronholm/apscheduler). Good.
- [Huey](https://github.com/coleifer/huey). Can [lose](https://github.com/coleifer/huey/issues/418) tasks.
- [Schedule](https://github.com/dbader/schedule). No concurrency, no persistence.
- [Litequeue](https://github.com/litements/litequeue). No concurrency, no scheduling.

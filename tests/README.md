# Schlange Unit Tests

This directory contains comprehensive unit tests for the Schlange task queue library.

## Running Tests

### Run all unit tests
```bash
make test-unit
```

Or directly with Python:
```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

### Run specific test file
```bash
python -m unittest tests.test_task -v
```

### Run specific test case
```bash
python -m unittest tests.test_task.TestTask.test_create_task -v
```

## Test Coverage

The test suite covers:

- **Core Domain Models**:
  - `Task` - Task lifecycle, execution, retries, and state transitions
  - `Schedule` - Schedule creation, firing, and recurring task generation
  - `RetryPolicy` - Exponential backoff and retry logic
  - `CleanupPolicy` - Task cleanup deadlines
  - `TaskExecution` - Task execution tracking
  - `ScheduleFiring` - Schedule firing tracking

## Test Structure

Each test file focuses on a specific component:

- `test_task.py` - Tests for the Task domain model
- `test_schedule.py` - Tests for the Schedule domain model
- `test_retry_policy.py` - Tests for RetryPolicy
- `test_cleanup_policy.py` - Tests for CleanupPolicy
- `test_task_execution.py` - Tests for TaskExecution
- `test_schedule_firing.py` - Tests for ScheduleFiring

## Dependencies

The unit tests use only Python's built-in `unittest` framework and have no external dependencies beyond the schlange package itself.

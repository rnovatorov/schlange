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
  - `Task` - Task lifecycle, execution, retries, and state transitions (14 tests)
  - `Schedule` - Schedule creation, firing, and recurring task generation (14 tests)
  - `RetryPolicy` - Exponential backoff and retry logic (7 tests)
  - `CleanupPolicy` - Task cleanup deadlines (3 tests)
  - `TaskExecution` - Task execution tracking (4 tests)
  - `ScheduleFiring` - Schedule firing tracking (4 tests)

- **Specification Classes**:
  - `TaskSpecification` - Task filtering logic (5 tests)
  - `ScheduleSpecification` - Schedule filtering logic (4 tests)

- **Background Workers**:
  - `Worker` - Base worker class with threading and lifecycle (6 tests)
  - `ExecutionWorker` - Task execution worker with thread pool (9 tests)
  - `CleanupWorker` - Task cleanup worker (6 tests)
  - `ScheduleWorker` - Schedule firing worker (9 tests)

**Total: 85 tests**

## Test Structure

Each test file focuses on a specific component:

- `test_task.py` - Tests for the Task domain model
- `test_schedule.py` - Tests for the Schedule domain model
- `test_retry_policy.py` - Tests for RetryPolicy
- `test_cleanup_policy.py` - Tests for CleanupPolicy
- `test_task_execution.py` - Tests for TaskExecution
- `test_schedule_firing.py` - Tests for ScheduleFiring
- `test_task_specification.py` - Tests for TaskSpecification
- `test_schedule_specification.py` - Tests for ScheduleSpecification
- `test_worker.py` - Tests for Worker base class
- `test_execution_worker.py` - Tests for ExecutionWorker
- `test_cleanup_worker.py` - Tests for CleanupWorker
- `test_schedule_worker.py` - Tests for ScheduleWorker

## Dependencies

The unit tests use only Python's built-in `unittest` framework and have no external dependencies beyond the schlange package itself.

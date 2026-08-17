# Contributing to Schlange

## Development

We use uv for managing development dependencies. See the Makefile for available commands.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for design decisions and [ROADMAP.md](ROADMAP.md) for build order. Package docstrings provide additional detail.

## Code Style

- Use `import schlange`, not `from schlange import ...`
- All imports at the top of the file
- No dead code — either use it or remove it
- Follow existing patterns in the codebase

## Design Process

Work is organized into phases tracked in [ROADMAP.md](ROADMAP.md). Design decisions are recorded in [ARCHITECTURE.md](ARCHITECTURE.md). Per phase: design → implement → verify → commit.

## Submitting Changes

1. Ensure tests and linting pass
2. Write tests for new functionality
3. Follow existing patterns — consistency over cleverness
4. Keep changes focused — one feature per commit

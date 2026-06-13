# Contributing to Schlange

## Development

We use pipenv for managing development dependencies. See the Makefile for available commands.

## Architecture

See package docstrings for architectural guidance.

## Code Style

- Use `import schlange`, not `from schlange import ...`
- All imports at the top of the file
- No dead code — either use it or remove it
- Follow existing patterns in the codebase

## Design Process

Major features are designed via specs in `specs/`. Specs are committed together with their implementation.

## Submitting Changes

1. Ensure tests and linting pass
2. Write tests for new functionality
3. Follow existing patterns — consistency over cleverness
4. Keep changes focused — one feature per commit

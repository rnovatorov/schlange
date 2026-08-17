.PHONY: default
default:


.PHONY: lint
lint: lint-black lint-isort lint-mypy lint-pyflakes

.PHONY: lint-black
lint-black:
	uv run black --check .

.PHONY: lint-isort
lint-isort:
	uv run isort --check-only .

.PHONY: lint-mypy
lint-mypy:
	uv run mypy .

.PHONY: lint-pyflakes
lint-pyflakes:
	uv run pyflakes src tests examples


.PHONY: test
test: test-examples test-unit

.PHONY: test-examples
test-examples:
	uv run python -m doctest -v examples/*.py

.PHONY: test-unit
test-unit:
	uv run python -m unittest discover -v

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


.PHONY: .ci-setup-env
.ci-setup-env:
	pip install pipenv
	pipenv install --dev


.PHONY: upload-to-pypi
upload-to-pypi: dist
ifndef PYPI_API_TOKEN
	$(error PYPI_API_TOKEN is not defined)
endif
	UV_PUBLISH_TOKEN=$(PYPI_API_TOKEN) uv publish

dist.tar: dist
	rm --force $@
	tar --create --file $@ $<

.PHONY: dist
dist:
	uv build

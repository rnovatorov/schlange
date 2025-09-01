.PHONY: default
default:

.PHONY: lint
lint: lint-black lint-isort lint-mypy

.PHONY: lint-black
lint-black:
	pipenv run black --check .

.PHONY: lint-isort
lint-isort:
	pipenv run isort --check-only .

.PHONY: lint-mypy
lint-mypy:
	pipenv run mypy .

.PHONY: .ci-setup-env
.ci-setup-env:
	pip install pipenv
	pipenv install --dev .

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
	pipenv install --dev

.PHONY: upload-to-pypi
upload-to-pypi: dist
ifndef PYPI_API_TOKEN
	$(error PYPI_API_TOKEN is not defined)
endif
	@pipenv run twine upload \
		--username __token__ \
		--password $(PYPI_API_TOKEN) \
		$</*

dist.tar: dist
	rm --force $@
	tar --create --file $@ $<

.PHONY: dist
dist:
	pipenv run python setup.py bdist_wheel

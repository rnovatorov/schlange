.PHONY: default
default:

.PHONY: .ci-setup-env
.ci-setup-env:
	pip install pipenv
	pipenv install --dev

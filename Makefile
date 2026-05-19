.PHONY: install precommit test test-unit test-integration

install:
	pip install -U pip
	pip install -e ".[dev]"
	pre-commit install
	pre-commit autoupdate

precommit:
	pre-commit run --all-files

test:
	pytest

test-unit:
	pytest lcpdelta_python_package/tests/unit

test-integration:
	pytest lcpdelta_python_package/tests/integration


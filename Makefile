.PHONY: test

install:
	pip install -U pip
	pip install -e ".[dev]"
	pre-commit install
	pre-commit autoupdate

precommit:
	pre-commit run --all-files


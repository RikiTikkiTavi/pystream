SHELL:=/usr/bin/env bash

.PHONY: lint
lint:
	poetry run mypy pystream

.PHONY: unit
unit:
	poetry run pytest

.PHONY: package
package:
	poetry check
	poetry run pip check

.PHONY: test
test: lint package unit
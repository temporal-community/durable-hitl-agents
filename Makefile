.PHONY: lint fmt test run

lint:
	ruff check . && ruff format --check .

fmt:
	ruff check --fix . && ruff format .

test:
	pytest

run:
	./run.sh

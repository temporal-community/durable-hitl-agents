.PHONY: install lint fmt test run

install:
	uv sync --all-extras

lint:
	uv run ruff check . && uv run ruff format --check .

fmt:
	uv run ruff check --fix . && uv run ruff format .

test:
	uv run pytest

run:
	./run.sh

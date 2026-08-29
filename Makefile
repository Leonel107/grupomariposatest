PYTHON = py -3.13
VENV = venv
VENV_PYTHON = $(VENV)/Scripts/python.exe
VENV_PIP = $(VENV)/Scripts/pip.exe

.PHONY: help venv install test lint format check clean

help:
	@echo "Available commands:"
	@echo "  make venv    - Create Python virtual environment"
	@echo "  make install - Install project and development dependencies"
	@echo "  make test    - Run tests"
	@echo "  make lint    - Run Ruff linter"
	@echo "  make format  - Format code with Ruff"
	@echo "  make check   - Run linter and tests"
	@echo "  make clean   - Remove generated Python files"

venv:
	$(PYTHON) -m venv $(VENV)

install:
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -e ".[dev]"

test:
	$(VENV_PYTHON) -m pytest

lint:
	$(VENV_PYTHON) -m ruff check .

format:
	$(VENV_PYTHON) -m ruff format .

check:
	$(VENV_PYTHON) -m ruff check .
	$(VENV_PYTHON) -m pytest

clean:
	@powershell -NoProfile -Command "Get-ChildItem -Path . -Recurse -Directory -Filter __pycache__ | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue"
	@powershell -NoProfile -Command "Get-ChildItem -Path . -Recurse -Directory -Filter .pytest_cache | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue"
	@powershell -NoProfile -Command "Get-ChildItem -Path . -Recurse -Directory -Filter .ruff_cache | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue"
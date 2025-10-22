# Makefile for process-redis-events Python package

.PHONY: install install-dev test lint format typecheck clean help

help:
	@echo "Available commands:"
	@echo "  make install      - Install package dependencies"
	@echo "  make install-dev  - Install package and dev dependencies"
	@echo "  make test         - Run tests"
	@echo "  make lint         - Run linting"
	@echo "  make format       - Format code with black"
	@echo "  make typecheck    - Run type checking with mypy"
	@echo "  make clean        - Clean build artifacts"

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pip install -e .

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=process_redis_events --cov-report=html --cov-report=term

lint:
	ruff check process_redis_events tests

format:
	black process_redis_events tests
	ruff check --fix process_redis_events tests

typecheck:
	mypy process_redis_events

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

test:
	poetry run pytest --cov=src/ --cov-report html

lint:
	poetry run ruff src/ tests/
	poetry run black --check src/ tests/

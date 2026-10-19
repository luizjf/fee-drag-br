.DEFAULT_GOAL := help
UV ?= uv

# Ingestion window, as `make ingest START=2021-01 END=2026-09`
START ?=
END ?=

.PHONY: help setup ingest build test app docs

help: ## Show the available targets
	@echo "fee-drag-br targets:"
	@echo "  make setup                         uv sync + pre-commit install"
	@echo "  make ingest START=2021-01 END=...  Python ingestion into data/bronze/"
	@echo "  make build                         dbt deps + dbt build"
	@echo "  make test                          ruff + pytest + dbt build on fixtures"
	@echo "  make app                           streamlit run app/streamlit_app.py"
	@echo "  make docs                          dbt docs generate"

setup: ## Install dependencies and git hooks
	$(UV) sync --all-groups
	$(UV) run pre-commit install

ingest: ## Download and land CVM/BCB data in the bronze layer
	@echo "not implemented yet - ingestion CLI lands in week 2 (START=$(START) END=$(END))"

build: ## Build the DuckDB warehouse with dbt
	@echo "not implemented yet - dbt project lands in week 2"

test: ## Lint, format check and unit tests
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run pytest
	@echo "not implemented yet - dbt build on fixtures lands in week 2"

app: ## Run the Streamlit app
	@echo "not implemented yet - Streamlit app lands in week 5"

docs: ## Generate dbt docs
	@echo "not implemented yet - dbt docs land in week 2"

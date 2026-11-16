.PHONY: install lint test check validate-bundle

install:            ## create the virtualenv
	uv sync

lint:               ## static checks
	uv run ruff check .

test:               ## unit tests
	uv run pytest

check: lint test    ## what CI runs

validate-bundle:    ## validate the Databricks bundle (needs PROFILE=<cli profile>)
	databricks bundle validate -t dev --profile $(PROFILE)

APPLICATION_MODULE=metabase_cli
TEST_MODULE=tests

.PHONY: activate
activate: ## activate the virtualenv associate with this project
	pipenv shell

# @see http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.DEFAULT_GOAL := help
.PHONY: help
help: ## provides cli help for this makefile (default)
	@grep -E '^[a-zA-Z_0-9-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: clean
clean : ## remove all transient directories and files
	rm -rf dist
	rm -rf *.egg-info
	find -name __pycache__ -print0 | xargs -0 sudo rm -rf
	pipenv --rm

.PHONY: dist
dist: ## create a package
	pipenv run python setup.py sdist

.PHONY: freeze_requirements
freeze_requirements: ## update the project dependencies based on setup.py declaration
	pipenv update

.PHONY: install_requirements
install_requirements: ## install the project dependencies based on setup.py
	pipenv install -e .

.PHONY: tests
tests: ## run automatic tests
	pipenv run pytest

.PHONY: start-backend
start-backend:  ## run backend using docker
	docker-compose up

.PHONY: start-metabase
start-metabase: ## run metabase using docker
	docker-compose -f docker-compose-with-metabase.yml up

.PHONY: create-enriched-views
create-enriched-views: ## connect to docker postgres database
	curl -X POST localhost:5000/?token=abc123

.PHONY: access-database
access-database: ## connect to docker postgres database
	docker exec -it analytics-datasource-postgres psql -U pass_culture

.PHONY: run-python
run-python:  ## run python using docker container
	docker exec -it analytics-datasource-application bash -c "cd /opt/data-analytics && PYTHONPATH=. python"


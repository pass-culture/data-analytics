APPLICATION_MODULE=metabase_cli
TEST_MODULE=tests

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

.PHONY: format
format : ## reformat files using Black
	docker exec -it analytics-datasource-application bash -c "cd /opt/data-analytics && black ."

.PHONY: dist
dist: ## create a package
	docker exec -it analytics-datasource-application bash -c "cd /opt/data-analytics && python setup.py sdist"

.PHONY: freeze-requirements
freeze_requirements: ## update the project dependencies based on setup.py declaration
	docker exec -it analytics-datasource-application bash -c "cd /opt/data-analytics && pip install -e ."

.PHONY: tests
tests: ## run automatic tests
	docker exec -it analytics-datasource-application bash -c "cd /opt/data-analytics && pytest"

.PHONY: start-backend
start-backend:  ## run backend using docker
	docker-compose up

.PHONY: start-metabase
start-metabase: ## run metabase using docker
	docker-compose -f docker-compose.yml -f docker-compose-with-metabase.yml up

.PHONY: initialize-metabase
initialize-metabase: ## create Metabase super user and setup database
	docker exec -it analytics-datasource-application bash -c  "cd /opt/data-analytics && python -m utils.initialize_metabase"

.PHONY: reset-metabase
reset-metabase: ## stop metabase delete metabase volume and mount it again
	docker container stop pcm-metabase-app pcm-postgres-metabase && docker container rm pcm-metabase-app pcm-postgres-metabase
	docker-compose -f docker-compose-with-metabase.yml up -d metabase-app

.PHONY: create-enriched-views
create-enriched-views: ## connect to docker postgres database local
	docker exec -it analytics-datasource-application bash -c  "cd /opt/data-analytics && pc-data-analytics create_enriched_views --local"

.PHONY: clean-database-and-view
clean-database-and-view: ## clean local database and view
	docker exec -it analytics-datasource-application bash -c  "cd /opt/data-analytics && python -m utils.clean_database_if_local"

.PHONY: access-database
access-database: ## connect to docker postgres database
	docker exec -it analytics-datasource-blue-postgres psql -U pass_culture

.PHONY: run-python
run-python:  ## run python using docker container
	docker exec -it analytics-datasource-application bash -c "cd /opt/data-analytics && python"


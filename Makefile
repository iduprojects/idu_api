CODE := idu_api
TEST := tests

lint:
	poetry run pylint $(CODE)

lint-tests:
	poetry run pylint $(TEST)

format:
	poetry run isort $(CODE)
	poetry run black $(CODE)

format-tests:
	poetry run isort $(TEST)
	poetry run black $(TEST)

database-docker:
	cd urban_api && docker compose up -d database

run-urban-api:
	CONFIG_PATH=urban_api/urban-api.config.yaml poetry run launch_urban_api

install:
	pip install .

install-dev:
	poetry install --with dev

install-dev-pip:
	pip install -e . --config-settings editable_mode=strict

clean:
	rm -rf ./dist

build:
	poetry build

install-from-build:
	python -m wheel install dist/idu_api-*.whl

prepare-migration:
	cd idu_api/common/db; CONFIG_PATH=../../../urban_api/urban-api.config.yaml poetry run alembic revision --autogen

apply-migrations:
	cd idu_api/common/db; CONFIG_PATH=../../../urban_api/urban-api.config.yaml poetry run alembic upgrade head

test-urban-api:
	poetry run pytest --verbose tests/urban_api/

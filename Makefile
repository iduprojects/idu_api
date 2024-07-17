CODE := idu_api

lint:
	poetry run pylint $(CODE)

format:
	poetry run isort $(CODE)
	poetry run black $(CODE)

database-docker:
	cd urban_api && docker compose up -d database

run-urban-api:
	ENVFILE=urban_api/.env poetry run launch_urban_api

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
	cd idu_api/common/db; poetry run alembic revision --autogen

apply-migrations:
	cd idu_api/common/db; poetry run alembic upgrade head

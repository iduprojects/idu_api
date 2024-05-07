CODE := urban_api

lint:
	poetry run pylint $(CODE)

format:
	poetry run isort $(CODE)
	poetry run black $(CODE)

run:
	poetry run launch_urban_api

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
	python -m wheel install dist/urban_api-*.whl

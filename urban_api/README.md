# Digital Territories Platform Data API

This is a Digital Territories Platform API to access and manipulate basic territories data.

## Running locally

### Preparation

1. To install python dependencies
  run `poetry install`. You may also want to use `venv` before that.
2. Prepare a PostgreSQL Server to store the database.
3. Go to ./test_fastapi/db and run `alembic upgrade head` to apply migrations. Do not forget to set environment variables
  `addr`, `port`, `name`, `user` and `password` in urban-api.config.yaml if they are different from default values.

### launching

Run backend locally with `poetry launch_urban_api` or `make run-urban-api`.

You can open [localhost:8000](http://localhost:8000) (or different host/port if you configured it) to get a redirect to Swagger UI with endpoints list.


## Running in docker 

1. Create urban-api.config.yaml by copying and editing urban-api.config.yaml.example.
2. Create .env file by copying and editing env.example (repeat the same thing with db.env).
3. Run the command `docker-compose up -d --build`
4. You can open [localhost:8000](http://localhost:8000) (or different host/port if you configured it) to get a redirect to Swagger UI with endpoints list.


## logging

Urban API uses structlog lib to write logs. When saving to file, it is formatted as jsonlines. You can use `pygmentize -l json <filename>`
to colorfully print results to terminal.

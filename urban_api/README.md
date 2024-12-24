# Digital Territories Platform Data API

This is a Digital Territories Platform API to access and manipulate basic territories data.

## Running locally

### Preparation

1. To install python dependencies
  run `poetry install`. You may also want to use `venv` before that.
2. Prepare a PostgreSQL Server to store the database.
3. Go to ./test_fastapi/db and run `alembic upgrade head` to apply migrations. Do not forget to set environment variables
  `DB_ADDR`, `DB_PORT`, `DB_NAME`, `DB_USER` and `DB_PASS` (or list them in .env file) if they are different from
  default values.

### launching

Run backend locally with `poetry launch_urban_api` or `poetry launch_urban_api --debug`.

You can open [localhost:8000](http://localhost:8000) (or different host/port if you configured it) to get a redirect to Swagger UI with endpoints list.

To get an access token (for example, you can import this into Postman):

`curl -X POST -d client_id=<client_id> -d client_secret=<secret> -d username=<user> -d password=<password> -d grant_type=password http://localhost:8080/realms/<realm>/protocol/openid-connect/token`


## Running in docker 

1. Create .env file by copying and editing env.example (repeat the same thing with db.env).
2. Run the command `docker-compose up -d --build`
3. You can open [localhost:8000](http://localhost:8000) (or different host/port if you configured it) to get a redirect to Swagger UI with endpoints list.


## logging

Urban API uses structlog lib to write logs. When saving to file, it is formatted as jsonlines. You can use `pygmentize -l json <filename>`
to colorfully print results to terminal.

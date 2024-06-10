FROM python:3.11-alpine

RUN apk add --virtual build-deps
RUN apk add python3-dev musl-dev linux-headers postgresql-dev geos-dev

RUN pip3 install --no-cache-dir poetry

COPY pyproject.toml /app/pyproject.toml
RUN sed -i '0,/version = .*/ s//version = "0.1.0"/' /app/pyproject.toml && touch /app/README.md

WORKDIR /app
RUN poetry config virtualenvs.create false
RUN poetry install

COPY README.md /app/README.md
COPY urban_api /app/urban_api

RUN pip3 install .

CMD ["launch_urban_api"]

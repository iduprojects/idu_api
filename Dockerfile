FROM python:3.11-alpine

RUN apk add --virtual build-deps
RUN apk add python3-dev musl-dev linux-headers postgresql-dev geos-dev

RUN pip3 install --no-cache-dir poetry

COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/pyproject

WORKDIR /app
RUN poetry install

COPY README.md /app/README.md
COPY urban_api /app/urban_api

RUN pip3 install .

CMD ["launch_urban_api"]

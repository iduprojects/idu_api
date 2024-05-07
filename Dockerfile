# -- builder --
FROM python:3.11-alpine AS builder

RUN apk add --virtual build-deps
RUN apk add python3-dev musl-dev linux-headers postgresql-dev

RUN pip3 install --no-cache-dir poetry

COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/pyproject

WORKDIR /app
RUN poetry install

COPY README.md /app/README.md
COPY urban_api /app/urban_api

RUN poetry build

# -- api --
FROM python:3.11-alpine

RUN mkdir /app

COPY --from=builder /app/dist/*.tar.gz /urban_api.tar.gz

RUN pip3 install /urban_api.tar.gz

CMD ["launch_urban_api"]

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
COPY chair_api /app/chair_api

RUN poetry build

# -- api --
FROM python:3.11-alpine

RUN mkdir /app

COPY --from=builder /app/dist/*.tar.gz /chair_api.tar.gz

RUN pip3 install /chair_api.tar.gz

CMD ["launch_urban_api"]

FROM python:3.11-alpine

RUN apk add --virtual build-deps
RUN apk add python3-dev musl-dev linux-headers postgresql-dev geos-dev

RUN pip3 install --no-cache-dir poetry

COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/pyproject

WORKDIR /app
RUN poetry install --with dev

COPY README.md /app/README.md
COPY urban_api /app/urban_api

RUN echo "cd /app/urban_api/db" > /entrypoint.sh && \
    echo "poetry run alembic upgrade head" >> /entrypoint.sh && \
    echo 'if [ $? = 0 ]; then echo "Database schema syncronized"; else echo "alembic upgrade has failed, database state is not determined"; exit 1; fi' >> /entrypoint.sh

ENTRYPOINT ["/bin/sh"]
CMD ["/entrypoint.sh"]

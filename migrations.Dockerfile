FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libpq-dev \
    libgeos-dev \
    git && \
    pip install --no-cache-dir poetry && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md /app/

RUN sed -i '0,/version = .*/ s//version = "0.1.0"/' pyproject.toml

RUN poetry config virtualenvs.create false && \
    poetry install --with dev

COPY urban-api.config.yaml /app/
COPY idu_api /app/idu_api

RUN pip install .

RUN echo "cd /app/idu_api/common/db" > /entrypoint.sh && \
    echo "poetry run alembic upgrade head" >> /entrypoint.sh && \
    echo 'if [ $? = 0 ]; then echo "Database schema synchronized"; else echo "alembic upgrade has failed, database state is not determined"; exit 1; fi' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

ENTRYPOINT ["/bin/sh"]
CMD ["/entrypoint.sh"]

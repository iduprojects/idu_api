FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libpq-dev \
    libgeos-dev \
    git \
    logrotate \
    curl && \
    pip install --no-cache-dir poetry && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md /app/

RUN sed -i '0,/version = .*/ s//version = "0.1.0"/' pyproject.toml

RUN poetry config virtualenvs.create false && \
    poetry install --with dev --no-root

COPY idu_api /app/idu_api

RUN pip install .

CMD ["launch_urban_api"]

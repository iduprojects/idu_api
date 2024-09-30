ARG PYTHON_VERSION=3.12.1
FROM python:${PYTHON_VERSION}-slim as base

ENV PYTHONDONTWRITEBYTECODE=1
ENV APP_ENV=production
ENV PYTHONUNBUFFERED=1

COPY ./idu_api /app/idu_api
COPY ./city_api/.env.production /app/city_api/.env.production
COPY ./pyproject.toml /app/pyproject.toml

WORKDIR /app

RUN python -m pip install --no-cache-dir poetry

RUN poetry config virtualenvs.create false
RUN poetry install

# Expose the port that the application listens on.
EXPOSE 8000

# Run the application.
RUN echo "cd /app" > /app/entrypoint.sh && \
    echo "python -m gunicorn idu_api.city_api.main:app --workers 2 --worker-class uvicorn.workers.UvicornWorker --bind=0.0.0.0:8000 --timeout 0" >> /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/bin/sh"]
CMD ["/app/entrypoint.sh"]
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONPATH=/app/src

WORKDIR /app

RUN pip install --no-cache-dir poetry==2.4.0

COPY pyproject.toml poetry.lock README.md ./
RUN poetry install --only main --no-root --no-interaction

COPY src ./src

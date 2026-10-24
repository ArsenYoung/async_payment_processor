# Payment Processing Service

Асинхронный сервис обработки платежей на FastAPI, PostgreSQL и RabbitMQ.

## Структура проекта

```text
src/payment_processor/
├── api/          # HTTP API
├── consumer/     # обработчик сообщений
├── core/         # конфигурация приложения
├── db/           # подключение к PostgreSQL
└── messaging/    # подключение к RabbitMQ
```

## Требования

- Docker;
- Docker Compose;
- Python 3.12+ и Poetry — для локального запуска без Docker.

## Запуск через Docker Compose

Создайте локальный файл настроек:

```bash
cp .env.example .env
```

Файл `.env` содержит локальные настройки и не добавляется в Git.

Соберите образы и запустите сервисы:

```bash
docker compose up -d --build
```

Проверьте состояние контейнеров:

```bash
docker compose ps
```

Ожидаемый результат:

- PostgreSQL — `healthy`;
- RabbitMQ — `healthy`;
- API — `healthy`;
- consumer — запущен.

Остановить сервисы:

```bash
docker compose down
```

## Доступные адреса

- API: <http://localhost:8000>
- Swagger UI: <http://localhost:8000/docs>
- Healthcheck: <http://localhost:8000/health>
- RabbitMQ Management UI: <http://localhost:15672>

Для RabbitMQ Management UI используются значения из `.env`. В шаблоне указаны `guest` и `change-me-rabbitmq`.

Проверка API:

```bash
curl http://localhost:8000/health
```

Ожидаемый ответ:

```json
{"status":"ok"}
```

## Локальный запуск через Poetry

Создайте виртуальное окружение и установите зависимости:

```bash
poetry install
```

Запустите API:

```bash
poetry run uvicorn payment_processor.api.main:app --reload
```

Для локального запуска без Docker PostgreSQL и RabbitMQ должны быть доступны по адресам из `.env`.

Запуск consumer:

```bash
poetry run python -m payment_processor.consumer.main
```

## Полезные команды

Проверить конфигурацию Poetry:

```bash
poetry check --lock
```

Посмотреть логи:

```bash
docker compose logs -f api
docker compose logs -f consumer
```

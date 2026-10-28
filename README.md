# Payment Processing Service

Асинхронный сервис обработки платежей на FastAPI, PostgreSQL и RabbitMQ.

## Текущий статус

Готовы:

- отдельные процессы API, публикатора Outbox и consumer;
- конфигурация через переменные окружения;
- Dockerfile и Docker Compose;
- подключение к PostgreSQL и RabbitMQ;
- модели `Payment` и `Outbox`;
- Alembic-миграция для таблиц `payments` и `outbox`;
- endpoint `GET /health`;
- публикатор Outbox с подтверждением RabbitMQ и корректной остановкой.

В следующих блоках будут добавлены обработчик платежей в consumer, отправка webhook, retry и DLQ.

## Фоновые процессы

| Сервис Compose | Роль | Точка запуска |
| --- | --- | --- |
| `outbox-to-rabbitmq-worker` | Выбирает события из PostgreSQL Outbox, публикует в RabbitMQ и сохраняет результат отправки | `payment_processor.outbox_to_rabbitmq_worker.main` |
| `rabbitmq-to-payment-worker` | Отдельный процесс для обработки сообщений `payments.new` | `payment_processor.rabbitmq_to_payment_worker.main` |

Воркеры используют один Docker-образ и запускаются отдельными командами. У каждого свои подключения и обработка сигналов остановки. Публикатор не зависит от запуска consumer. Сейчас consumer подключается к RabbitMQ и запускает инфраструктуру подписчиков; обработчик платежей будет добавлен следующим блоком.

Настройки публикатора: `OUTBOX_BATCH_SIZE` — размер партии, `OUTBOX_POLL_INTERVAL` — пауза между партиями в секундах, `OUTBOX_PUBLISH_TIMEOUT` — таймаут публикации одного события в секундах. Значения по умолчанию: `100`, `1.0` и `5.0`; все значения должны быть положительными, интервалы и таймауты — конечными.

## Структура проекта

```text
src/payment_processor/
├── api/                         # HTTP API
├── core/                        # конфигурация приложения
├── db/                          # подключение и модели PostgreSQL
├── messaging/                   # подключение к RabbitMQ
├── outbox_to_rabbitmq_worker/    # публикатор PostgreSQL Outbox → RabbitMQ
├── rabbitmq_to_payment_worker/  # обработчик сообщений
└── services/                    # операции с платежами и публикация Outbox

alembic/
└── versions/     # миграции базы данных
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
- `outbox-to-rabbitmq-worker` — запущен;
- `rabbitmq-to-payment-worker` — запущен.

Остановить сервисы:

```bash
docker compose down
```

## Доступные адреса

- API: <http://localhost:8000>
- Swagger UI: <http://localhost:8000/docs>
- ReDoc: <http://localhost:8000/redoc>
- OpenAPI: <http://localhost:8000/openapi.json>
- Healthcheck: <http://localhost:8000/health>
- RabbitMQ Management UI: <http://localhost:15672>

Для RabbitMQ Management UI используются значения из `.env`. В шаблоне указаны `guest` и `change-me-rabbitmq`.

Операции API и `/health` требуют заголовок `X-API-Key`. Swagger UI, ReDoc и OpenAPI доступны без ключа.

Для выполнения запросов в Swagger нажмите **Authorize** и введите значение `API_KEY` из `.env`. Swagger будет передавать его в заголовке `X-API-Key`.

Для проверки API задайте переменную `API_KEY` в терминале тем же значением, что в `.env`:

```bash
export API_KEY="<значение API_KEY из .env>"
curl -H "X-API-Key: ${API_KEY}" http://localhost:8000/health
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

Запустите публикатор Outbox в отдельном терминале:

```bash
poetry run python -m payment_processor.outbox_to_rabbitmq_worker.main
```

Запустите consumer в другом терминале:

```bash
poetry run python -m payment_processor.rabbitmq_to_payment_worker.main
```

## Миграции базы данных

Применить все миграции:

```bash
poetry run alembic upgrade head
```

Откатить последнюю миграцию:

```bash
poetry run alembic downgrade -1
```

Показать текущую версию базы:

```bash
poetry run alembic current
```

Создать новую миграцию после изменения моделей:

```bash
poetry run alembic revision --autogenerate -m "describe schema change"
```

## Проверки качества

Проверить стиль и ошибки в коде:

```bash
poetry run ruff check src alembic
```

Проверить форматирование:

```bash
poetry run ruff format --check src alembic
```

Проверить типы:

```bash
poetry run mypy
```

Запустить тесты:

```bash
poetry run pytest
```

Тесты включают проверки API, Outbox, RabbitMQ и жизненного цикла отдельных воркеров.

## Полезные команды

Проверить конфигурацию Poetry:

```bash
poetry check --lock
```

Посмотреть логи:

```bash
docker compose logs -f api
docker compose logs -f outbox-to-rabbitmq-worker
docker compose logs -f rabbitmq-to-payment-worker
```

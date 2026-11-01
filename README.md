# Payment Processing Service

Асинхронный процессинг платежей на FastAPI, PostgreSQL, SQLAlchemy и RabbitMQ/FastStream. API атомарно сохраняет платеж и Outbox, публикатор передаёт событие в `payments.new`, один consumer вызывает шлюз, сохраняет результат и отправляет webhook.

Реализованы требования [задания](docs/task.md): идемпотентный POST, GET платежа, эмуляция шлюза с задержкой 2–5 секунд и распределением 90%/10%, Outbox, три попытки с экспоненциальной задержкой, DLQ, API-ключ, миграции и Docker Compose. Исход симулятора стабилен для UUID платежа.

## Быстрый запуск

Нужны Docker Engine и Compose v2.

```bash
cp .env.example .env
```

Укажите в `.env` собственные `POSTGRES_PASSWORD`, `RABBITMQ_PASSWORD`, `API_KEY`. Для локального запуска строки `DATABASE_URL` и `RABBITMQ_URL` должны соответствовать этим значениям; специальные символы в URL требуют percent-encoding.

```bash
docker compose up -d --build --wait
```

Сервис `migrate` применяет миграции перед API и workers. API доступен на `http://localhost:8000`, RabbitMQ Management — на `http://localhost:15672` с учётными данными из `.env`.

```bash
export API_KEY='ваш-ключ-из-.env'
curl -H "X-API-Key: $API_KEY" http://localhost:8000/health
```

Все эндпоинты, включая `/health`, `/docs`, `/redoc`, `/docs/oauth2-redirect` и `/openapi.json`, требуют `X-API-Key`; отсутствующий или неверный ключ даёт `401`. Для загрузки страницы документации передайте заголовок через клиент или расширение браузера. HTML содержит OpenAPI-схему. Для API-запросов из Swagger введите ключ через **Authorize**.

Остановка: `docker compose down`. Именованные тома сохраняют данные PostgreSQL и RabbitMQ; `down --volumes` удаляет их.

## Конфигурация

Полный пример — [.env.example](.env.example). При запуске через Compose адреса БД и брокера внутри контейнеров задаёт Compose; при запуске через Poetry используются `DATABASE_URL`, `RABBITMQ_URL` и `API_KEY` из окружения или `.env`.

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `LOG_LEVEL` | `INFO` | Уровень JSON-логов |
| `OUTBOX_BATCH_SIZE` | `100` | Максимум событий за проход |
| `OUTBOX_POLL_INTERVAL` | `1.0` | Пауза после прохода, секунды |
| `OUTBOX_PUBLISH_TIMEOUT` | `5.0` | Ожидание подтверждения публикации |
| `WEBHOOK_TIMEOUT` | `5.0` | Таймаут фаз HTTP-запроса |
| `WEBHOOK_REQUEST_DEADLINE` | `5.0` | Общий срок HTTP-запроса |
| `RETRY_BASE_DELAY` | `1.0` | Задержка первого повтора |
| `RETRY_MAX_ATTEMPTS` | `3` | Число попыток, включая первую |
| `RETRY_BACKOFF_MULTIPLIER` | `2.0` | Множитель задержки |
| `CONSUMER_PREFETCH_COUNT` | `5` | Максимум неподтверждённых сообщений |

Lease обработки и webhook действует 60 секунд, ожидание захвата — до 5 секунд с проверками через 250 мс. Внешний вызов ограничен 30 секундами. Подробности — в [границах транзакций](docs/transaction-boundaries.md).

## API

Создание платежа требует уникального `Idempotency-Key` длиной 1–255 символов:

```bash
curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"10.50","currency":"RUB","description":"Order 42","metadata":{"order_id":"42"},"webhook_url":"https://example.com/webhook"}'
```

Ответ `202` содержит `payment_id`, `status` и `created_at`. Для проверки фактической доставки укажите доступный consumer HTTP(S)-адрес получателя; `example.com` служит примером.

Повтор с тем же ключом и данными возвращает существующий платеж, в том числе его текущий статус. Другие данные с тем же ключом дают `409`. Конкурентные запросы сохраняют один платеж и одно событие.

Сумма — положительный decimal в пределах `NUMERIC(18,2)`; валюты — `RUB`, `USD`, `EUR`. Описание содержит 1–500 символов, URL — до 2048 символов после нормализации. Неизвестные поля, некорректный JSON, нулевые символы и нечисловые суммы отклоняются с `422`.

```bash
curl -H "X-API-Key: $API_KEY" \
  http://localhost:8000/api/v1/payments/00000000-0000-0000-0000-000000000000
```

GET возвращает данные платежа, `status`, `created_at`, `processed_at`; неизвестный UUID даёт `404`. Статусы: `pending`, `succeeded`, `failed`.

## Обработка и гарантии доставки

1. Общая транзакция создаёт Payment и Outbox.
2. `outbox-to-rabbitmq-worker` публикует `payment.created` с persistent delivery, стабильным `event_id`/`message_id` и положительным publisher confirm.
3. `rabbitmq-to-payment-worker` сохраняет результат шлюза и отправляет webhook с `X-Webhook-Delivery-Id: <payment UUID>`.
4. При повторяемой ошибке consumer выполняет три попытки с задержками 1 и 2 секунды; затем передаёт событие в `payments.dlq`. Неповторяемые ошибки попадают в DLQ сразу. Исходное сообщение подтверждается после подтверждённой передачи в retry/DLQ.

`failed` — завершённый бизнес-результат шлюза, который также доставляется webhook. При retry завершённого платежа шлюз повторно не вызывается. Все ответы webhook вне `2xx`, сетевые ошибки, timeout и временные ошибки БД допускают retry. Redirect не выполняется.

Гарантия — **at-least-once**. При внешнем успехе и сбое commit публикация или webhook могут повториться с прежним идентификатором. Lease защищает запись результата от устаревшего владельца; шлюз и получатель webhook должны обеспечивать идемпотентность внешнего эффекта. Exactly-once не заявляется.

Произвольные webhook URL предполагают доверенных клиентов и сетевые ограничения. Политика защиты от SSRF для публичного развёртывания требует отдельного решения.

## Локальная разработка и проверки

Нужны Python 3.12 и Poetry. Настройте `.env` и отдельные тестовые PostgreSQL/RabbitMQ; тесты создают и удаляют данные. Docker-тесты используют собственные Compose-проекты со случайными портами.

```bash
poetry install
poetry run alembic upgrade head
poetry run uvicorn payment_processor.api.main:app --reload
```

В отдельных терминалах:

```bash
poetry run python -m payment_processor.outbox_to_rabbitmq_worker.main
poetry run python -m payment_processor.rabbitmq_to_payment_worker.main
```

```bash
poetry run ruff check .
poetry run ruff format --check .
poetry run mypy
```

Каждый тест имеет ровно один уровень: `unit`, `integration`, `smoke` или `e2e`. Pytest проверяет марки при сборе.

| Уровень | Команда | Зависимости |
| --- | --- | --- |
| Application unit | `poetry run pytest -q tests/unit` | Без `.env` и внешних сервисов; fake-порты и mock |
| Все unit | `poetry run pytest -q -m unit` | Тестовые настройки для импорта смешанных модулей; внешние сервисы не нужны |
| Integration | `poetry run pytest -q -m integration` | Тестовые PostgreSQL/RabbitMQ и миграции |
| Smoke | `RUN_DOCKER_SMOKE=1 poetry run pytest -q -m smoke` | Docker: запуск, миграции, сохранность данных |
| E2E | `RUN_E2E=1 poetry run pytest -q -m e2e` | Docker: полный поток платежа и webhook |

Для сбора смешанных модулей задайте `DATABASE_URL`, `RABBITMQ_URL`, `API_KEY` через окружение или `.env`; для unit допустимы адреса недоступных сервисов. Fake-хранилище проверяет последовательные сценарии и rollback; конкурентность и блокировки проверяются с PostgreSQL. Контрактные тесты сопоставляют fake и SQLAlchemy, проверяют подтверждение публикации и поведение HTTP-клиента.

Полный unit/integration-прогон и отдельные Docker-прогоны:

```bash
poetry run pytest -q
RUN_DOCKER_SMOKE=1 poetry run pytest -q tests/smoke
RUN_E2E=1 poetry run pytest -q tests/e2e
```

На одноразовой тестовой БД проверьте `alembic upgrade head`, `alembic downgrade base`, повторный `alembic upgrade head` и `alembic check`. В CI это отдельный job; Docker smoke/e2e выполняются отдельными jobs с сохранением логов и очисткой окружения.

Сценарии и порты находятся в `application`, сущность и инварианты — в `domain`, адаптеры — в `db`, `messaging` и `adapters`; сборка зависимостей — в `bootstrap.py`.

Подробности: [транзакции и leases](docs/transaction-boundaries.md), [Outbox и восстановление](docs/outbox.md), [retry и DLQ](docs/retry-and-dlq.md), [контракт webhook](docs/webhook-delivery.md), [логи и диагностика](docs/diagnostics.md).

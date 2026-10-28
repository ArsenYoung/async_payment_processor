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
- создание платежей через `POST /api/v1/payments` и получение через `GET /api/v1/payments/{payment_id}`;
- идемпотентное создание платежа и атомарное сохранение платежа вместе с событием Outbox;
- защита операций API и `GET /health` ключом `X-API-Key`, публичная документация с авторизацией в Swagger;
- публикатор Outbox с подтверждением RabbitMQ и корректной остановкой;
- эмулятор шлюза: задержка 2–5 секунд, 90% успешных и 10% неуспешных результатов;
- обработка платежа в `PaymentService`: сохранение итогового статуса и времени обработки, защита от повторного вызова шлюза для завершённого платежа, включая конкурентные вызовы;
- HTTP-клиент webhook: отправка сохранённого результата платежа, таймаут и проверка ответа получателя;
- подписчик `payments.new`: обработка платежа, доставка webhook и сохранение `webhook_delivered_at`.

После POST публикатор передаёт событие из Outbox в `payments.new`, consumer сохраняет результат шлюза и отправляет webhook. Расширенные проверки повторной доставки, retry и DLQ остаются следующими шагами плана.

## Фоновые процессы

| Сервис Compose | Роль | Точка запуска |
| --- | --- | --- |
| `outbox-to-rabbitmq-worker` | Выбирает события из PostgreSQL Outbox, публикует в RabbitMQ и сохраняет результат отправки | `payment_processor.outbox_to_rabbitmq_worker.main` |
| `rabbitmq-to-payment-worker` | Обрабатывает `payments.new`, сохраняет результат платежа, отправляет webhook и отмечает доставку | `payment_processor.rabbitmq_to_payment_worker.main` |

Воркеры используют один Docker-образ и запускаются отдельными командами. У каждого свои подключения и обработка сигналов остановки. Публикатор не зависит от запуска consumer.

Платёж и событие Outbox создаются в одной транзакции. Публикатор отправляет устойчивое сообщение в очередь `payments.new` через direct exchange `payments` и отмечает событие опубликованным после подтверждения RabbitMQ. При сбое между подтверждением и commit возможна повторная публикация с тем же `event_id`; для завершённого платежа сервис повторно шлюз не вызывает.

Настройки публикатора: `OUTBOX_BATCH_SIZE` — размер партии, `OUTBOX_POLL_INTERVAL` — пауза между партиями в секундах, `OUTBOX_PUBLISH_TIMEOUT` — таймаут публикации одного события в секундах. Значения по умолчанию: `100`, `1.0` и `5.0`; все значения должны быть положительными, интервалы и таймауты — конечными.

## Структура проекта

```text
src/payment_processor/
├── api/                         # HTTP API
├── core/                        # конфигурация приложения
├── db/                          # модели, репозитории и сессии PostgreSQL
├── messaging/                   # схемы событий и топология RabbitMQ
├── outbox_to_rabbitmq_worker/    # публикатор PostgreSQL Outbox → RabbitMQ
├── rabbitmq_to_payment_worker/  # обработчик сообщений
└── services/
    ├── payment_service.py
    ├── outbox_service.py
    └── payment_gateway_simulator.py

alembic/
└── versions/     # миграции базы данных
```

`PaymentService` объединяет создание, чтение, обработку платежа и доставку webhook; проверка идемпотентности находится в его закрытых методах. `OutboxService` публикует сохранённые события в RabbitMQ. `PaymentGatewaySimulator` эмулирует внешний шлюз и передаётся в `PaymentService` как зависимость.

При обработке сервис блокирует строку платежа через `SELECT … FOR UPDATE`, вызывает шлюз только для `pending` и сохраняет `succeeded` или `failed` вместе с `processed_at` в UTC. Блокировка удерживается до завершения транзакции. Конкурентный вызов для того же платежа ждёт и затем возвращает сохранённый результат. Техническая ошибка приводит к откату транзакции и передаётся вызывающему коду.

Один подписчик принимает валидируемый `PaymentCreatedEvent` и использует `payment_id` для чтения платежа из БД. Сумма, результат и адрес webhook берутся из сохранённого платежа. Неизвестный платёж приводит к `LookupError`; ошибки зависимостей передаются FastStream.

## Требования

- Docker;
- Docker Compose;
- Python 3.12+ и Poetry — для миграций, тестов и локального запуска. В текущем Docker-образе нет файлов Alembic, поэтому миграции выполняются с хоста.

## Webhook результата платежа

`WebhookClient` отправляет POST на сохранённый `webhook_url` с JSON из завершённого платежа. Пример payload:

```json
{
  "payment_id": "d183f5bb-994a-4a68-a433-206ac244b3d2",
  "status": "succeeded",
  "amount": "10.50",
  "currency": "RUB",
  "description": "Оплата заказа",
  "metadata": {"order_id": "demo-order-001"},
  "created_at": "2026-10-28T10:00:00Z",
  "processed_at": "2026-10-28T10:00:03Z"
}
```

Статус — `succeeded` или `failed`, сумма передаётся строкой без потери точности. Даты и остальные данные берутся из сохранённого платежа. Незавершённый результат отклоняется до HTTP-запроса.

Успехом считается любой ответ `2xx`; тело ответа не требуется. Перенаправления не выполняются. Остальные HTTP-ответы вызывают `httpx.HTTPStatusError`; сетевые ошибки и таймауты также передаются вызывающему коду. Клиент сам не повторяет отправку. Он используется через `async with`, которое закрывает HTTP-ресурсы при выходе, включая ошибки и отмену задачи.

`WEBHOOK_TIMEOUT` по умолчанию равен `5.0` секунды и должен быть положительным конечным числом. Он задаёт таймауты подключения, чтения, записи и ожидания соединения в пуле. Consumer создаёт один `WebhookClient(timeout=settings.webhook_timeout)` на время работы и закрывает его после остановки брокера, затем освобождает ресурсы БД. `httpx` входит в основные зависимости, устанавливаемые Dockerfile через `poetry install --only main`.

Результат шлюза фиксируется до отправки webhook. Доставка выполняется в отдельной транзакции: сервис заново читает платёж с `SELECT … FOR UPDATE`, проверяет `webhook_delivered_at`, отправляет webhook и сохраняет отметку только после ответа `2xx`. При наличии отметки отправка пропускается. Ошибка уведомления откатывает только транзакцию доставки, сохраняя итоговый статус и `processed_at`.

На текущем шаге используется `AckPolicy.REJECT_ON_ERROR`: сообщение подтверждается после успешного завершения обработчика и commit отметки доставки; при ошибке отклоняется без возврата в очередь. Retry и DLQ ещё не настроены, поэтому отклонённое сообщение удаляется. Сбой между успешным HTTP-запросом и commit отметки допускает повтор webhook при повторной доставке события. Ограничение числа попыток и маршрутизация ошибок будут реализованы в блоке 7.

## Запуск через Docker Compose

Создайте локальный файл настроек:

```bash
cp .env.example .env
```

Файл `.env` содержит локальные настройки и не добавляется в Git.

Установите зависимости для запуска миграций с хоста:

```bash
poetry install
```

Запустите PostgreSQL и RabbitMQ и дождитесь их готовности:

```bash
docker compose up -d --wait postgres rabbitmq
```

Примените миграции к БД из `DATABASE_URL` в `.env`. В шаблоне указан PostgreSQL на `localhost:5432`; при изменении имени БД или учётных данных обновите также `DATABASE_URL`.

```bash
poetry run alembic upgrade head
```

После миграций соберите образы и запустите API и воркеры:

```bash
docker compose up -d --build
```

Миграции при запуске контейнеров автоматически не применяются.

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

## API платежей

### Создание платежа

Для нового платежа нужны `X-API-Key` и `Idempotency-Key`. Задайте `API_KEY`, как показано выше, и выполните запрос:

```bash
export IDEMPOTENCY_KEY="demo-order-001"
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: ${API_KEY}" \
  -H "Idempotency-Key: ${IDEMPOTENCY_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"amount":"10.50","currency":"RUB","description":"Оплата заказа","metadata":{"order_id":"demo-order-001"},"webhook_url":"https://example.com/webhook"}'
```

Первый запрос возвращает `202 Accepted`. Пример тела ответа; идентификатор и время будут другими:

```json
{
  "payment_id": "d183f5bb-994a-4a68-a433-206ac244b3d2",
  "status": "pending",
  "created_at": "2026-10-28T10:00:00Z"
}
```

Повтор с тем же ключом и теми же данными возвращает тот же платёж с кодом `202`, его текущим статусом и исходным `created_at`. Новые платёж и событие Outbox не создаются. Если данные изменились, сервер возвращает `409 Conflict`. Для другого платежа используйте новый ключ.

### Получение платежа

Скопируйте `payment_id` из ответа POST в переменную `PAYMENT_ID`:

```bash
export PAYMENT_ID="d183f5bb-994a-4a68-a433-206ac244b3d2"
curl -i -H "X-API-Key: ${API_KEY}" \
  "http://localhost:8000/api/v1/payments/${PAYMENT_ID}"
```

Успешный ответ — `200 OK`. Пример для созданного платежа:

```json
{
  "payment_id": "d183f5bb-994a-4a68-a433-206ac244b3d2",
  "status": "pending",
  "created_at": "2026-10-28T10:00:00Z",
  "amount": "10.50",
  "currency": "RUB",
  "description": "Оплата заказа",
  "metadata": {"order_id": "demo-order-001"},
  "idempotency_key": "demo-order-001",
  "webhook_url": "https://example.com/webhook",
  "processed_at": null
}
```

Для GET заголовок `Idempotency-Key` не нужен. API поддерживает статусы `pending`, `succeeded`, `failed`; для завершённого платежа `processed_at` содержит время обработки. При работающих публикаторе и consumer платёж завершается автоматически; ошибка webhook не меняет его итоговый статус.

### Валидация и ошибки

Сумма должна быть положительной, содержать не более 18 цифр и не более двух знаков после точки. Допустимые валюты: `RUB`, `USD`, `EUR`. Описание обязательно, от 1 до 500 символов. `metadata` — JSON-объект, по умолчанию `{}`. `webhook_url` обязателен: HTTP(S)-адрес длиной до 2048 символов. Дополнительные поля запроса отклоняются. `Idempotency-Key` — непустая строка длиной до 255 символов.

| Код | Причина |
| --- | --- |
| `401 Unauthorized` | Отсутствует или неверен `X-API-Key` |
| `404 Not Found` | В GET указан корректный UUID, но платёж не найден |
| `409 Conflict` | Ключ идемпотентности уже использован с другими данными |
| `422 Unprocessable Entity` | Неверное тело POST, отсутствует или некорректен `Idempotency-Key`, либо некорректен UUID в GET |

## Локальный запуск через Poetry

Создайте `.env` из `.env.example`, если ещё не сделали этого, и установите зависимости:

```bash
poetry install
```

PostgreSQL и RabbitMQ должны быть доступны по адресам из `.env`. Для локальных контейнеров используйте `docker compose up -d --wait postgres rabbitmq`.

До запуска приложения примените миграции:

```bash
poetry run alembic upgrade head
```

Запустите API:

```bash
poetry run uvicorn payment_processor.api.main:app --reload
```

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
poetry run ruff check .
```

Проверить форматирование:

```bash
poetry run ruff format --check .
```

Проверить типы:

```bash
poetry run mypy
```

Запустить тесты:

```bash
poetry run pytest -q
```

Перед запуском тестов настройте `.env`, запустите PostgreSQL и RabbitMQ и примените миграции. Интеграционные тесты используют реальные подключения: часть создаёт записи с уникальными ключами в настроенной БД и удаляет их после проверки, часть — временные схемы PostgreSQL и отдельные очереди и обменники RabbitMQ.

Покрыты создание и получение платежей, авторизация, валидация, идемпотентность и конкурентные запросы, атомарность платежа и Outbox, публикация и восстановление после сбоев, жизненный цикл API и воркеров. Тесты эмулятора проверяют задержки и оба результата без реального ожидания; тесты обработки — сохранение результата, повторные и конкурентные вызовы, откат при ошибках шлюза, commit и отмене задачи.

После объединения сервисов полный запуск завершился успешно: **236 сценариев**. Для шага 6.5 связанный набор `tests/test_payment_consumer.py`, `tests/test_rabbitmq_to_payment_worker.py`, `tests/test_payment_processing.py`, `tests/test_webhooks.py`, `tests/test_webhook_delivery_state.py` прошёл **104 сценария**, включая обработку через настоящий RabbitMQ и PostgreSQL. Проверены оба результата шлюза, сохранённые данные webhook, ошибки событий и зависимостей, сохранность результата при сбоях webhook и закрытие ресурсов. HTTP в тестах webhook заменён через `httpx.MockTransport`; полный запуск Docker-сценария остаётся в блоке 8.

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

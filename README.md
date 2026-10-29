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
- подписчик `payments.new`: обработка платежа, доставка webhook и сохранение `webhook_delivered_at`;
- автоматические retry с экспоненциальной задержкой и DLQ;
- общие JSON-логи API и воркеров с идентификаторами запросов, платежей и событий.

После POST публикатор передаёт событие из Outbox в `payments.new`, consumer сохраняет результат шлюза и отправляет webhook. При временной ошибке consumer планирует повтор; после исчерпания трёх попыток событие отправляется в `payments.dlq`. Неповторяемые ошибки направляются в DLQ сразу.

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

Один подписчик принимает валидируемый `PaymentCreatedEvent` и использует `payment_id` для чтения платежа из БД. Сумма, результат и адрес webhook берутся из сохранённого платежа. Неизвестный платёж приводит к `PaymentNotFoundError`, совместимому с `LookupError`; обработчик классифицирует ошибки и передаёт событие в retry или DLQ.

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

Обработчик использует функцию `is_retryable_processing_error` в `services/errors.py` для выбора retry или DLQ. `failed` остаётся обычным результатом шлюза: он сохраняется и отправляется клиенту. Для повторной попытки подходят исключения HTTP-доставки, включая любой ответ вне `2xx`, ошибки соединения и таймауты.

Для БД учитываются потеря соединения, исчерпание пула и SQLSTATE ошибок соединения, отката транзакции, нехватки ресурсов, недоступности блокировки и остановки сервера. Если SQLSTATE отсутствует, `OperationalError` и `InterfaceError` считаются повторяемыми; прочие неизвестные ошибки БД — нет. Известные ошибки ограничений, авторизации и SQL не повторяются.

Отсутствующий платёж вызывает `PaymentNotFoundError`, совместимый с `LookupError`; ошибки валидации и программирования не подходят для retry и приводят к передаче события в DLQ. Отмена задачи распространяется без передачи в retry/DLQ.

Используется `AckPolicy.NACK_ON_ERROR`: исходное сообщение подтверждается после успешного завершения обработчика и commit отметки доставки либо после подтверждённой RabbitMQ публикации в retry/DLQ. Если передача в retry/DLQ не удалась, исключение передаётся FastStream, а исходное сообщение возвращается в очередь. Всего предусмотрено три попытки обработки: первая и два повтора через `payments.retry.2` и `payments.retry.3`. Задержки равны `RETRY_BASE_DELAY` и удвоенному значению; по умолчанию это 1 и 2 секунды. После задержки событие возвращается в `payments.new`. Сбой между успешным HTTP-запросом и commit отметки допускает повтор webhook при повторной доставке события.

Получатель webhook должен обрабатывать повторные уведомления идемпотентно по `payment_id`: успешный HTTP-ответ и сохранение отметки доставки в нашей БД не составляют одну атомарную операцию. При повторной доставке события завершённый платёж сохраняет статус и `processed_at`, шлюз не вызывается заново. Если отметки доставки нет, webhook отправляется снова с теми же данными; после commit отметки повторные события пропускают отправку. При конкурентной доставке второй обработчик ждёт блокировку строки и проверяет отметку после завершения транзакции первого.

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

После шага 6.6 полный запуск завершился успешно: **330 сценариев**. Связанный набор `tests/test_payment_consumer.py`, `tests/test_rabbitmq_to_payment_worker.py`, `tests/test_payment_processing.py`, `tests/test_webhooks.py`, `tests/test_webhook_delivery_state.py` прошёл **116 сценариев**, включая обработку через настоящий RabbitMQ и PostgreSQL. Проверены оба результата шлюза, сохранённые данные webhook, ошибки событий и зависимостей, сохранность результата при сбоях webhook и закрытие ресурсов. Добавлены 12 сценариев повторных событий после успеха, восстановления доставки после HTTP 503, ошибки соединения или таймаута, повторной отправки после сбоя commit отметки и конкурентной доставки с ожиданием блокировки PostgreSQL. Повторы сохраняют исходный результат платежа и не вызывают шлюз заново; после commit отметки webhook больше не отправляется. HTTP в тестах webhook заменён через `httpx.MockTransport`; в сценариях шага 6.6 повтор события задавался явно. Полный запуск Docker-сценария остаётся в блоке 8.

## Логирование и диагностика

### Уровень и формат логов

API и оба воркера пишут в stdout: одна JSON-запись на строку. `LOG_LEVEL` задаёт минимальный уровень, по умолчанию `INFO`. Допустимы `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`; регистр и пробелы по краям нормализуются, неизвестное значение отклоняется при загрузке настроек. Compose передаёт настройку всем трём процессам. При локальном запуске она читается из окружения или `.env`; изменение применяется при следующем запуске процесса. После изменения `.env` в Docker выполните:

```bash
docker compose up -d api outbox-to-rabbitmq-worker rabbitmq-to-payment-worker
```

`INFO` подходит для поиска всей истории платежа: видны HTTP-запросы, запуск и остановка воркеров, публикация, обработка и доставка. `WARNING` показывает временные ошибки и запланированные повторы; `ERROR` — в том числе передачу в DLQ и сбой публикации в retry/DLQ. Уровни `WARNING` и выше скрывают успешные этапы. Обычные HTTP-ответы `401`, `404`, `409` и `422` записываются на `INFO`; необработанные исключения API — на `ERROR`. Пустые опросы Outbox не создают записей. Для библиотек подключений и HTTP-клиента минимальный уровень — `WARNING`, даже при `LOG_LEVEL=DEBUG`.

| Поля | Значение |
| --- | --- |
| `timestamp`, `level` | Время UTC с миллисекундами и суффиксом `Z`; уровень записи |
| `component` | `api`, `outbox-to-rabbitmq-worker` или `rabbitmq-to-payment-worker` |
| `logger`, `message` | Имя логгера и описание действия |
| `request_id` | Сгенерированный API идентификатор HTTP-запроса; возвращается в заголовке ответа `X-Request-ID` |
| `method`, `path`, `status_code` | Метод, путь без query-параметров и код HTTP-ответа; для `webhook_http` код относится к получателю webhook |
| `duration_ms` | Длительность соответствующего запроса или операции в миллисекундах |
| `payment_id`, `event_id` | Идентификаторы платежа и события Outbox; связывают этапы обработки и повторы |
| `worker` | Имя воркера в записях запуска и остановки |
| `queue`, `operation`, `outcome` | Очередь, этап и его результат; в `retry_publish`/`dlq_publish` очередь обозначает назначение публикации |
| `publication_attempt` | Номер попытки публикации Outbox, начиная с 1; соответствует увеличению `outbox.attempts` при сохранении результата |
| `attempt` | Текущая попытка consumer/webhook от 1 до 3; независима от `publication_attempt` |
| `next_attempt`, `retry_delay_ms` | Следующая попытка и задержка перед ней; присутствуют при публикации в retry |
| `payment_status` | Сохранённый результат платежа: `succeeded` или `failed` |
| `reason`, `error_type` | Категория причины и имя класса исключения |
| `exception` | Тип исключения и traceback: файл, строка, функция; при наличии — цепочка причин или вложенные исключения |

Поля `timestamp`, `level`, `component`, `logger`, `message` присутствуют в каждой записи; остальные добавляются при наличии контекста. API записывает `payment_id`, когда он известен маршруту, но не передаёт `request_id` в событие Outbox. Поэтому переход от HTTP к воркерам выполняется по `payment_id`, а между Outbox, consumer и повторами — также по `event_id`. При некорректном сообщении `payment_id` может отсутствовать; `event_id` берётся из `message_id`, если это корректный UUID.

### Просмотр и поиск

Команды выполняются из корня проекта. Для фильтрации ниже нужен `rg` (ripgrep) на хосте. Посмотреть последние записи трёх процессов и продолжить наблюдение:

```bash
docker compose logs --tail=100 --follow api outbox-to-rabbitmq-worker rabbitmq-to-payment-worker
```

Для отдельного этапа укажите только нужный сервис:

```bash
docker compose logs --since=15m --tail=100 outbox-to-rabbitmq-worker
docker compose logs --since=15m --tail=100 rabbitmq-to-payment-worker
```

Найти историю платежа за последний час. Замените вымышленный UUID значением `payment_id` из ответа POST:

```bash
PAYMENT_ID='d183f5bb-994a-4a68-a433-206ac244b3d2'
docker compose logs --since=1h --no-color --no-log-prefix api outbox-to-rabbitmq-worker rabbitmq-to-payment-worker \
  | rg -F -- "$PAYMENT_ID"
```

В найденной записи `outbox_publish` или `consume` возьмите `event_id`, чтобы проследить конкретное событие, включая retry и DLQ:

```bash
EVENT_ID='a1e6cf84-7baa-4cea-8a93-787b8886d6ee'
docker compose logs --since=1h --no-color --no-log-prefix outbox-to-rabbitmq-worker rabbitmq-to-payment-worker \
  | rg -F -- "$EVENT_ID"
```

Найти запрос API, в том числе ошибку до определения платежа, можно по `X-Request-ID` из ответа:

```bash
REQUEST_ID='9307e7ca-32e2-4bc8-a880-37a714de4e20'
docker compose logs --since=1h --no-color --no-log-prefix api \
  | rg -F -- "$REQUEST_ID"
```

`--no-color` и `--no-log-prefix` убирают оформление Compose, сохраняя JSON приложения. Интервал `--since` следует увеличить, если платёж создан раньше. Отсутствие записей также возможно при уровне выше `INFO` или после удаления контейнера и его логов.

### Как читать историю платежа

| Этап (`operation`) | Результат (`outcome`) и смысл |
| --- | --- |
| HTTP-запрос API, без `operation` | `HTTP request completed`: проверяйте `status_code`, `request_id` и доступный `payment_id` |
| `outbox_publish` | `published`: RabbitMQ подтвердил публикацию и транзакция Outbox завершилась; `failed`: ошибка публикации сохранена; `transaction_failed`: транзакция не завершилась, счётчик попыток мог откатиться |
| `consume` | `received`: сообщение принято; `completed`: обработка и доставка завершились; `failed`: обработчик выбрал retry или DLQ по `reason` |
| `payment_process` | `processed`: результат шлюза сохранён; `already_processed`: использован сохранённый результат без нового вызова шлюза; `failed`: техническая ошибка |
| `webhook_http` | `accepted`: получен `2xx`; `failed`: ошибка запроса или ответ вне `2xx`, при полученном ответе есть `status_code` |
| `webhook_delivery` | `delivered`: отметка доставки сохранена после commit; `already_delivered`: отправка пропущена по сохранённой отметке; `failed`: доставка или её сохранение завершились ошибкой |
| `retry_publish` | `published`: RabbitMQ подтвердил передачу в очередь следующей попытки; `failed`: передача не подтверждена |
| `dlq_publish` | `published`: RabbitMQ подтвердил передачу в `payments.dlq`; запись имеет уровень `ERROR`; `failed`: передача в DLQ не удалась |

`payment_status=failed` — завершённый бизнес-результат шлюза. Он допускает успешную доставку webhook и `consume` с `outcome=completed`.

Пример записи об успешном HTTP-запросе API; все идентификаторы и времена в примерах вымышлены:

```json
{"timestamp":"2026-10-29T10:00:00.125Z","level":"INFO","component":"api","logger":"payment_processor.api.middleware","message":"HTTP request completed","request_id":"9307e7ca-32e2-4bc8-a880-37a714de4e20","method":"POST","path":"/api/v1/payments","status_code":202,"duration_ms":12.5,"payment_id":"d183f5bb-994a-4a68-a433-206ac244b3d2"}
```

Если первая отправка webhook получила `503`, ищите `webhook_http` с `outcome=failed`, `status_code=503`, `error_type=HTTPStatusError`, затем `webhook_delivery` и `consume` с ошибкой. Подтверждённый повтор для того же платежа и события выглядит так:

```json
{"timestamp":"2026-10-29T10:00:03.250Z","level":"WARNING","component":"rabbitmq-to-payment-worker","logger":"payment_processor.messaging.topology","message":"Payment retry scheduled","payment_id":"d183f5bb-994a-4a68-a433-206ac244b3d2","event_id":"a1e6cf84-7baa-4cea-8a93-787b8886d6ee","queue":"payments.retry.2","operation":"retry_publish","outcome":"published","attempt":1,"next_attempt":2,"retry_delay_ms":1000.0,"reason":"retryable_processing_error","error_type":"HTTPStatusError","duration_ms":4.0}
```

При следующей обработке `consume` показывает `attempt=2`, а `payment_process` — `already_processed`: итог платежа уже сохранён. После успешного webhook ожидаются `webhook_http` с `accepted`, `webhook_delivery` с `delivered` и `consume` с `completed`. Сам по себе `webhook_http` с `accepted` ещё не подтверждает commit отметки доставки; при сбое commit возможна повторная отправка.

### Диагностика сбоев

- **Outbox:** найдите `outbox_publish` с `failed`, его `error_type` и `exception`. Проверьте доступность RabbitMQ и объявление очереди `payments.new`; после восстановления ожидается `published` с тем же `event_id` и увеличенным `publication_attempt`. При `transaction_failed` результат не зафиксирован, а подтверждённая брокером публикация могла уже произойти. `outbox_poll` с `reason=database_unavailable` указывает на недоступность БД; такая запись может не содержать идентификаторов платежа или события.
- **Webhook:** по `webhook_http` различайте HTTP-ошибку с `status_code` и сетевую ошибку/таймаут по `error_type`. Проверьте доступность получателя из сети consumer и его собственные логи. Ошибка webhook не отменяет сохранённый статус платежа; если HTTP принят, но `webhook_delivery` завершилась ошибкой, проверьте сохранение в БД.
- **Retry:** `consume` с `reason=retryable_processing_error` означает выбор повтора. Его подтверждает только `retry_publish` с `outcome=published`, очередью `payments.retry.2` или `payments.retry.3`, `next_attempt` и `retry_delay_ms`. Задержка относится к ожиданию в retry; время до начала обработки зависит также от очереди и доступности consumer. Повторные доставки при сбое публикации или подтверждения могут повторять один номер `attempt`.
- **DLQ:** `reason=attempts_exhausted` означает, что временная ошибка сохранилась на третьей попытке; `non_retryable_processing_error` — ошибку без повтора; `invalid_message` — некорректное тело события или заголовок попытки. Ищите подтверждение `dlq_publish` с `outcome=published`, `queue=payments.dlq` и тем же `event_id`. В RabbitMQ Management UI откройте очередь `payments.dlq`, чтобы проверить число сообщений; автоматического возврата из неё нет.
- **Сбой передачи в retry/DLQ:** `retry_publish` или `dlq_publish` с `outcome=failed` показывает ошибку самой публикации в `error_type` и `exception`. Запись `consume` с ошибкой ещё не доказывает передачу. Исходное сообщение возвращается в очередь; проверьте RabbitMQ и дождитесь записи `published` либо успешного завершения обработки.

### Чувствительные данные

Прикладные записи не включают ключи и секретные заголовки, пароли и строки подключения с учётными данными, `metadata`, тела запросов и ответов, payload webhook и полный webhook URL. Query-параметры HTTP-запроса не записываются. Форматтер маскирует известные значения настроек `API_KEY`, `DATABASE_URL`, `RABBITMQ_URL`, пароли из этих URL, адреса URL и чувствительные поля в сообщениях.

В `exception` сохраняются типы и расположение кадров traceback; тексты исключений, строки исходного кода и локальные переменные исключены. Для диагностики используйте `error_type`, `reason`, `status_code`, идентификаторы и кадры traceback. `DEBUG` не отключает фильтрацию чувствительных данных.

## Полезные команды

Проверить конфигурацию Poetry:

```bash
poetry check --lock
```

Посмотреть логи отдельных процессов; поиск по идентификаторам описан в разделе «Логирование и диагностика»:

```bash
docker compose logs -f api
docker compose logs -f outbox-to-rabbitmq-worker
docker compose logs -f rabbitmq-to-payment-worker
```

# Логирование и диагностика

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


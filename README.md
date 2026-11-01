# Payment Processing Service

Асинхронный сервис процессинга платежей на FastAPI, PostgreSQL и RabbitMQ/FastStream. При создании платежа сервис сохраняет событие в Outbox, обрабатывает платёж через эмулятор шлюза и отправляет результат на webhook. [Текст задания](docs/task.md).

## Запуск

Нужны Docker Engine и Compose v2.

```bash
cp .env.example .env
```

Задайте в `.env` свои `POSTGRES_PASSWORD`, `RABBITMQ_PASSWORD` и `API_KEY`, затем запустите сервис:

```bash
docker compose up -d --build --wait
```

Compose применяет миграции перед запуском API и воркеров. API доступен на `http://localhost:8000`. Все эндпоинты требуют заголовок `X-API-Key`.

```bash
export API_KEY='ваш-ключ-из-.env'
curl -H "X-API-Key: $API_KEY" http://localhost:8000/health
```

Остановка: `docker compose down`. Данные в именованных томах сохраняются.

## API

Создать платёж:

```bash
curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"10.50","currency":"RUB","description":"Order 42","metadata":{"order_id":"42"},"webhook_url":"https://example.com/webhook"}'
```

Ответ `202 Accepted` содержит `payment_id`, статус и время создания. Для доставки webhook укажите доступный сервису адрес получателя: `example.com` в примере служит заглушкой. Повтор с тем же ключом и данными вернёт существующий платёж; другие данные с тем же ключом дадут `409`.

Получить платёж (подставьте `payment_id` из ответа):

```bash
curl -H "X-API-Key: $API_KEY" \
  http://localhost:8000/api/v1/payments/<payment_id>
```

## Гарантии доставки

Платёж и запись Outbox создаются в одной транзакции. Публикатор передаёт событие в `payments.new`, consumer обрабатывает платёж и отправляет webhook. При повторяемой ошибке предусмотрены три попытки с экспоненциальной задержкой, затем сообщение попадает в DLQ. Доставка имеет гарантию **at-least-once**: получатель webhook должен устранять повторы по `X-Webhook-Delivery-Id`.

## Документация

- [Настройка, локальная разработка, проверки и эксплуатация](docs/operations.md)
- [Outbox и восстановление](docs/outbox.md)
- [Retry и DLQ](docs/retry-and-dlq.md)
- [Границы транзакций](docs/transaction-boundaries.md)
- [Доставка webhook](docs/webhook-delivery.md)
- [Логирование и диагностика](docs/diagnostics.md)

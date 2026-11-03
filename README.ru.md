# Payments Service

[![CI](https://github.com/Gegirhasut/paystream/actions/workflows/ci.yml/badge.svg)](https://github.com/Gegirhasut/paystream/actions/workflows/ci.yml)

Микросервис асинхронной обработки платежей. Принимает запрос на оплату, обрабатывает его
через эмулятор платёжного шлюза и сообщает клиенту результат через webhook.

**Стек:** FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL 16, RabbitMQ 3.13
(FastStream), Alembic, Docker Compose, pytest.

## Быстрый старт

Нужны Docker и Docker Compose v2, а также свободные порты `8000`, `5432`, `5672`, `15672`
и `8080` (их можно поменять в `.env`).

```bash
git clone https://github.com/Gegirhasut/paystream.git && cd paystream
cp .env.example .env
docker compose up -d --build        # или: make up
```

Первый запуск занимает 1–3 минуты. Миграции применяются автоматически.

```bash
curl http://localhost:8000/health   # {"status":"ok"}

curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me" -H "Idempotency-Key: first-payment" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.00", "currency": "RUB", "webhook_url": "http://webhook-receiver:8080/webhooks"}'

# через ~5 секунд
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: change-me"
curl http://localhost:8080/webhooks  # webhook'и, которые получил демо-мерчант
```

| Что         | Где                                                                  |
|-------------|----------------------------------------------------------------------|
| Swagger UI  | <http://localhost:8000/docs>: **Authorize**, ключ `change-me`        |
| RabbitMQ UI | <http://localhost:15672> (guest / guest): очереди, retry, DLQ        |
| Логи        | `docker compose logs -f api outbox-relay consumer`                   |
| Остановить  | `docker compose down -v` (или `make down`)                           |

## API

Все эндпоинты `/api/v1/*` требуют заголовок `X-API-Key` (`API_KEY` из `.env`).

**`POST /api/v1/payments`** принимает обязательный заголовок `Idempotency-Key` и тело:

| Поле          | Описание                                                  |
|---------------|-----------------------------------------------------------|
| `amount`      | > 0, не больше 2 знаков после запятой: `"1500.00"` или число |
| `currency`    | `RUB`, `USD` или `EUR`                                    |
| `description` | необязательное, до 1024 символов                          |
| `metadata`    | необязательный JSON-объект, до 16 КБ                      |
| `webhook_url` | http/https URL для результата                             |

Ответы:
- `202` и `{"payment_id", "status": "pending", "created_at"}`. Повтор с тем же ключом
  и телом возвращает тот же платёж с заголовком `Idempotent-Replayed: true`.
- `409`: ключ уже использован с другим телом.
- `401`: нет ключа или он неверный.
- `422`: ошибка валидации.
- `413`: тело больше 1 МБ.

**`GET /api/v1/payments/{payment_id}`** возвращает платёж со статусом (`pending`,
`succeeded` или `failed`) и `processed_at`. Если платёж не найден, ответ `404`.

**Webhook.** После обработки consumer отправляет `POST` на `webhook_url` с событием
`payment.processed` (статус, сумма, даты). Доставка at-least-once, поэтому мерчанту
стоит дедуплицировать по `payment_id`. Запрос подписан HMAC-SHA256 секретом
`WEBHOOK_SECRET` по строке `<X-Webhook-Timestamp>.<тело>`, подпись лежит в
`X-Webhook-Signature`. Пример проверки есть в `webhook_receiver/main.py`.

**Защита от SSRF.** Webhook уходит только на публичные адреса. Хост резолвится, и
запрос идёт ровно на проверенный IP, так что DNS rebinding не сработает. Редиректы и
настройки прокси из окружения не используются. Внутренние хосты разрешаются только
явным списком `WEBHOOK_ALLOWED_INTERNAL_HOSTS` (в compose там `webhook-receiver`).

## Важные моменты

- **Событие не теряется.** API пишет платёж и событие в таблицу `outbox` одной
  транзакцией и в RabbitMQ не ходит. Отдельный процесс `outbox-relay` публикует события
  и помечает их опубликованными только после подтверждения брокера. Consumer один, с
  одним обработчиком: шлюз, статус, webhook, 3 попытки с задержкой 2 и 4 с, затем DLQ.
- **Деньги не списываются дважды.** Consumer блокирует строку платежа
  (`SELECT … FOR UPDATE`). Повторная или параллельная доставка события видит финальный
  статус и шлюз не вызывает: повторяется только то, что не удалось (обычно webhook).
- **Неизвестный исход списания.** Если шлюз не ответил (таймаут, падение), платёж
  остаётся `pending`, а не становится `failed`, потому что деньги могли быть списаны.
  Повтор безопасен: `payment.id` служит ключом идемпотентности шлюза.
- **Отказ шлюза (10%) — это результат, а не ошибка.** Платёж получает статус `failed`,
  клиент получает webhook, ретраев нет.
- **Точность суммы.** Сумма хранится как `NUMERIC(18, 2)` и нигде не проходит через
  `float`. Это касается и JSON-чисел: `1234567890123456.78` сохраняется точно.
- **Платёж не зависает.** Relay периодически ищет незавершённые платежи без прогресса
  (по умолчанию 10 минут) и отправляет их на обработку заново. Это закрывает случаи,
  когда сообщение пропало из RabbitMQ. Таймауты PostgreSQL снимают блокировки зависших
  транзакций. Watchdog перезапускает consumer, если брокер снял подписку.

## Отступление от ТЗ

`GET /health` и Swagger работают без `X-API-Key`, хотя ТЗ требует ключ «для всех
эндпоинтов». Health check вызывает инфраструктура (Docker, балансировщик,
Kubernetes), и раздавать ей клиентский ключ незачем. Данных эти эндпоинты не
раскрывают. В продакшене их стоит закрыть на уровне сети. Если нужно строгое
соответствие ТЗ, закрыть их ключом — минутное дело. Вопрос стоит согласовать.

## Демонстрация retry и DLQ

Демо-мерчант `webhook-receiver` предоставляет три эндпоинта:
- `/webhooks` всегда отвечает `200`;
- `/webhooks/flaky` первые 2 раза отвечает `500`, затем `200`;
- `/webhooks/fail` всегда отвечает `500`, и после 3 попыток сообщение уходит в DLQ.

```bash
for path in webhooks webhooks/flaky webhooks/fail; do
  curl -s -X POST http://localhost:8000/api/v1/payments \
    -H "X-API-Key: change-me" -H "Content-Type: application/json" \
    -H "Idempotency-Key: demo-$path-$RANDOM" \
    -d "{\"amount\": \"100.00\", \"currency\": \"USD\", \"webhook_url\": \"http://webhook-receiver:8080/$path\"}"
done
sleep 15
docker compose logs consumer | grep -E "Attempt|DLQ"
```

Outbox тоже можно проверить. Остановите брокер (`docker compose stop rabbitmq`) и
создайте платёж: API ответит `202`. После `docker compose start rabbitmq` событие
будет опубликовано.

## Конфигурация

Все параметры задаются переменными окружения. Полный список с проверками лежит в
`app/config.py`, основные вынесены в `.env.example`. Главные:
- `API_KEY`, `WEBHOOK_SECRET`;
- `PROCESSING_MIN_DELAY` / `PROCESSING_MAX_DELAY` / `PROCESSING_SUCCESS_RATE`;
- `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_DELAY`.

С несогласованными значениями сервис не стартует.

## Тесты

```bash
make test    # 195 тестов на настоящих PostgreSQL и RabbitMQ в Docker
make lint    # ruff
```

Тесты используют отдельную БД и отдельный vhost, данные сервиса они не трогают.
CI на GitHub запускает линтер и тесты на каждый push.

## Ограничения

- Шлюз и webhook вызываются под блокировкой строки. Так проще, но каждый платёж в
  работе держит соединение из пула. Под high-load нужен промежуточный статус
  `processing`, а ТЗ задаёт ровно три статуса.
- Клиент один, мерчантов нет, поэтому `Idempotency-Key` и `WEBHOOK_SECRET` общие на
  весь сервис.
- Для продакшена не хватает метрик, алерта на непустую DLQ и инструмента переотправки
  сообщений из DLQ.

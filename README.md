# Ozon Warehouse SaaS

Сервис для учета складских остатков, упаковки, резервирования поставок и синхронизации товаров Ozon между несколькими магазинами.

## Стек

- Backend: FastAPI, SQLAlchemy, PostgreSQL, Redis
- Frontend: Next.js 14, TypeScript
- Background jobs: Celery worker + Celery beat
- Infra: Nginx, Prometheus, Grafana, Alertmanager
- Integrations: Ozon API, Telegram bot

## Что есть в проекте

- учет приходов, упаковки, резервов, отгрузок и возвратов
- режимы склада `shared` и `per_store`
- группировка товаров и вариаций по цвету, размеру и pack size
- фоновая синхронизация магазинов и bootstrap-sync при старте
- dashboard, shipments, matching и warehouse-products
- healthchecks и observability-стек для backend, worker, postgres, redis и nginx

## Быстрый старт

1. Скопируйте переменные окружения:

```bash
cp .env.example .env
cp frontend/.env.example frontend/.env.local
```

2. Заполните минимум:

- `JWT_SECRET_KEY`
- `ENCRYPTION_KEY`
- `TELEGRAM_TOKEN` и `TELEGRAM_CHAT_ID`, если нужны Telegram-уведомления
- `OZON_CLIENT_ID` и `OZON_API_KEY`, если хотите подключать тестовый магазин из `.env`

3. Поднимите сервисы:

```bash
docker compose up --build -d
```

4. Проверьте состояние:

```bash
docker compose ps
docker compose logs --tail=100 backend frontend worker beat bot
```

## Локальные адреса

- Приложение: `http://localhost:3000`
- Backend healthcheck: `http://localhost:3000/api/healthz`
- Backend readiness: `http://localhost:3000/api/readyz`
- Grafana: `http://localhost:3001`
- Prometheus: `http://localhost:9090`
- Alertmanager: `http://localhost:9093`

## Полезные команды

Frontend:

```bash
cd frontend
npm install
npm run typecheck
npm run lint
```

Backend и фоновые сервисы:

```bash
python3 -m compileall backend/app backend/tests worker bot
```

Если backend-зависимости установлены локально:

```bash
cd backend
pytest
```

## Структура

- `backend/` - API, модели, сервисы, тесты
- `frontend/` - Next.js UI
- `worker/` - Celery worker и beat
- `bot/` - Telegram bot
- `observability/` - Prometheus, Alertmanager, Grafana dashboards
- `docs/` - заметки по observability и production deploy

## Дополнительная документация

- `docs/observability.md` - метрики, алерты и адреса сервисов мониторинга
- `docs/DEPLOY.md` - production-контур, docker compose prod и миграции

## Замечания

- Backend при `AUTO_CREATE_SCHEMA=true` поднимает схему автоматически.
- При старте приложения запускается проверка runtime-настроек и очередь bootstrap sync для активных магазинов.
- В production лучше отключать `AUTO_CREATE_SCHEMA` и использовать Alembic-миграции из `backend/`.

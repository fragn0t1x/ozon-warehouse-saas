# Production Deploy

## Что подготовлено

- Production compose: `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/docker-compose.prod.yml`
- Production env template: `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/.env.prod.example`
- Production Dockerfiles:
  - `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/backend/Dockerfile.prod`
  - `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/frontend/Dockerfile.prod`
  - `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/worker/Dockerfile.prod`
  - `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/bot/Dockerfile.prod`
- Alembic scaffold:
  - `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/backend/alembic.ini`
  - `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/backend/alembic/env.py`

## Базовый запуск

1. Скопируйте `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/.env.prod.example` в `.env.prod` и заполните секреты.
2. Убедитесь, что `JWT_SECRET_KEY`, `POSTGRES_PASSWORD` и `ENCRYPTION_KEY` заменены на реальные значения.
3. Убедитесь, что `AUTO_CREATE_SCHEMA=false`, чтобы production не создавал таблицы через `create_all`.
3. Поднимите сервисы:

```bash
docker compose -f docker-compose.prod.yml --env-file .env.prod up --build -d
```

## Проверка здоровья

```bash
docker compose -f docker-compose.prod.yml ps
docker compose -f docker-compose.prod.yml logs --tail=100 backend frontend worker beat bot
```

## Миграции

Теперь production-контур рассчитан на Alembic: backend container на старте выполняет `alembic upgrade head`, а не полагается на `create_all`.

Создать ревизию:

```bash
cd backend
alembic revision --autogenerate -m "init schema"
```

Применить миграции:

```bash
cd backend
alembic upgrade head
```

Если база уже была создана раньше без Alembic и схема совпадает с первой миграцией, можно один раз привязать её к текущей ревизии:

```bash
cd backend
alembic stamp head
```

## Бэкапы

Минимальная стратегия:

1. Ежедневный dump Postgres.
2. Хранение минимум 7-14 последних копий.
3. Отдельная проверка восстановления хотя бы раз в месяц.

Пример ручного backup:

```bash
docker compose -f docker-compose.prod.yml exec -T postgres \
  pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > backup-$(date +%F).sql
```

## Что ещё стоит сделать перед боем

1. Поставить reverse proxy с TLS, например Nginx или Traefik.
2. Ограничить доступ к `3000` и не публиковать внутренние сервисы наружу без нужды.
3. Вынести секреты в secret manager, а не хранить только в `.env.prod`.
4. Добавить CI-проверку миграций и отдельный staging smoke-test.

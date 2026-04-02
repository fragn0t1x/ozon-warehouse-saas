# Observability

## Services

- Grafana: `http://localhost:3001`
- Prometheus: `http://localhost:9090`
- Alertmanager: `http://localhost:9093`

## Default Grafana Credentials

- Login: `admin`
- Password: `admin`

## Scrape Targets

- Backend metrics: `backend:8000/metrics`
- Worker metrics: `worker:9808/metrics`
- PostgreSQL exporter: `postgres_exporter:9187`
- Redis exporter: `redis_exporter:9121`
- Nginx exporter: `nginx_exporter:9113`

## Notes

- Backend exposes custom request counters and latency histograms.
- Backend also exposes business metrics for stores, supplies, next timeslots, stock units, notification coverage and recent admin events.
- Worker exposes Celery task throughput and runtime histograms.
- Nginx metrics are collected through `stub_status` and scraped by `nginx-prometheus-exporter`.
- Basic alert rules are configured in `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/observability/prometheus/alerts.yml`.
- Current rules cover backend down/5xx/latency, overdue supplies, stuck supply notification queue, worker down/retry spikes, postgres down/high connections, redis down/high memory and nginx down.
- Alerts are visible in Prometheus on `http://localhost:9090/alerts`.
- Delivery to Telegram is configured through Alertmanager using `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID` from `.env`.
- Alertmanager config template is stored in `/Users/andrejcernysov/Desktop/ozon_warehouse_saas/observability/alertmanager/alertmanager.yml.tpl`.
- Grafana now has two dashboards: infrastructure overview and `Ozon Business Overview`.

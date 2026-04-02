from celery import Celery
from celery.schedules import crontab
import os

redis_url = os.getenv("REDIS_URL", "redis://redis:6379")

celery = Celery("beat", broker=redis_url, backend=redis_url)

celery.conf.update(
    beat_schedule={
        'reserve-ready-supplies': {
            'task': 'worker.tasks.reserve_ready_supplies_task',
            'schedule': crontab(minute='*/2'),
        },
        'check-supplies-status': {
            'task': 'worker.tasks.check_supplies_status_task',
            'schedule': crontab(minute='*/3'),
        },
        'check-losses': {
            'task': 'worker.tasks.check_losses_task',
            'schedule': crontab(minute='*/10'),
        },
        'admin-overdue-supplies-digest': {
            'task': 'worker.tasks.admin_overdue_supplies_digest_task',
            'schedule': crontab(minute='*/10'),
        },
        'price-risk-alerts': {
            'task': 'worker.tasks.price_risk_alerts_task',
            'schedule': crontab(minute='*/30'),
        },
        'daily-report': {
            'task': 'worker.tasks.daily_report_task',
            'schedule': crontab(minute='*'),
        },
        'monthly-closed-month-report': {
            'task': 'worker.tasks.monthly_closed_month_report_task',
            'schedule': crontab(minute='*/10'),
        },
        'today-supplies': {
            'task': 'worker.tasks.today_supplies_task',
            'schedule': crontab(minute='*'),
        },
        'deliver-supply-notifications': {
            'task': 'worker.tasks.deliver_supply_notifications_task',
            'schedule': crontab(minute='*'),
        },
        'sync-products': {
            'task': 'worker.tasks.sync_products_all',
            'schedule': crontab(minute='*/30'),
        },
        'sync-supplies': {
            'task': 'worker.tasks.sync_supplies_all',
            'schedule': crontab(minute='*/5'),
        },
        'sync-stocks': {
            'task': 'worker.tasks.sync_stocks_all',
            'schedule': crontab(minute='*/5'),
        },
        'sync-report-snapshots': {
            'task': 'worker.tasks.sync_report_snapshots_all',
            'schedule': crontab(minute='*/15'),
        },
        'sync-finance-snapshots': {
            'task': 'worker.tasks.sync_finance_snapshots_all',
            'schedule': crontab(minute='*/15'),
        },
    },
    timezone='Europe/Moscow',  # Меняем на московское время
)

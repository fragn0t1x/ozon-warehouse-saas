FROM python:3.12

WORKDIR /app

# Копируем requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Копируем код worker
COPY . /app/worker

# Устанавливаем PYTHONPATH
ENV PYTHONPATH=/app

RUN adduser --disabled-password --gecos "" --uid 10001 appuser \
    && chown -R appuser:appuser /app

USER appuser

# Запускаем Celery worker
CMD ["celery", "-A", "worker.worker", "worker", "--loglevel=info", "--concurrency=1"]

FROM python:3.12

# Устанавливаем PostgreSQL клиент
RUN apt-get update && apt-get install -y postgresql-client netcat-openbsd && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все файлы проекта
COPY . .

# Копируем wait-for-postgres.sh в корень
COPY wait-for-postgres.sh /wait-for-postgres.sh
RUN chmod +x /wait-for-postgres.sh

# Копируем скрипты
COPY scripts/*.py /app/scripts/

# Запускаем с полным путем к скрипту
CMD ["/wait-for-postgres.sh", "postgres", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
#!/bin/bash
set -e

host="$1"
shift
cmd="$@"

# Ждём, пока PostgreSQL станет доступен
until PGPASSWORD=ozon psql -h "$host" -U ozon -d ozon -c '\q' 2>/dev/null; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 2
done

>&2 echo "Postgres is up - executing command"
exec $cmd
#!/bin/bash
set -e

export HOME=/tmp
export IVY_HOME=/tmp/.ivy2

echo "=================================================="
echo "Ожидание инициализации баз данных..."
echo "=================================================="

echo "Ожидание PostgreSQL (порт 5432)..."
while ! nc -z postgres_dw 5432; do
  sleep 2
done
echo "PostgreSQL готов!"

echo "Ожидание ClickHouse (порт 8123)..."
while ! nc -z clickhouse-db 8123; do
  sleep 2
done
echo "ClickHouse готов!"

echo "Ожидание MongoDB (порт 27017)..."
while ! nc -z mongodb_reports 27017; do
  sleep 2
done
echo "MongoDB готова!"

SPARK_SUBMIT="/opt/spark/bin/spark-submit"
JARS=$(ls /opt/spark/extra-jars/*.jar | tr '\n' ',' | sed 's/,$//')

echo "=================================================="
echo "ШАГ 1: Запуск трансформации сырых данных в 'Звезду' (PostgreSQL)"
echo "=================================================="
sleep 10
$SPARK_SUBMIT \
  --master "local[*]" \
  --jars "$JARS" \
  /opt/spark/work-dir/scripts/transform.py

echo "=================================================="
echo "ШАГ 2: Формирование витрин данных в ClickHouse и MongoDB"
echo "=================================================="
$SPARK_SUBMIT \
  --master "local[*]" \
  --jars "$JARS" \
  /opt/spark/work-dir/scripts/reports.py

echo "=================================================="
echo "ETL Пайплайн успешно завершен! Среда готова."
echo "=================================================="
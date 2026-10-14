# One image used by every service (simulator, Spark job, dashboard, tests).
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    PIP_NO_CACHE_DIR=1 \
    SPARK_LOCAL_IP=127.0.0.1

# Java is required by Spark
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# Download the Spark-Kafka connector jars now, so the first run does not need to.
# (If this step fails the jars are simply downloaded at first start instead.)
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]').config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3').getOrCreate().stop()" \
 || echo "connector pre-download skipped"

COPY . /app

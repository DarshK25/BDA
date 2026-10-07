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

# # Multi-stage Dockerfile for Python + Java + Spark environment
# FROM python:3.9-slim as base

# # Install system dependencies
# RUN apt-get update && apt-get install -y \
#     openjdk-17-jdk \
#     wget \
#     curl \
#     procps \
#     && rm -rf /var/lib/apt/lists/*

# # Set Java environment
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV PATH="${JAVA_HOME}/bin:${PATH}"

# # Install Apache Spark
# ENV SPARK_VERSION=3.5.0
# ENV HADOOP_VERSION=3
# ENV SPARK_HOME=/opt/spark
# ENV PATH="${SPARK_HOME}/bin:${PATH}"
# ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

# RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
#     && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
#     && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}" \
#     && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# # Set working directory
# WORKDIR /app

# # Copy requirements and install Python dependencies
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy application code
# COPY src/ ./src/
# COPY conf/ ./conf/
# COPY scripts/ ./scripts/

# # Copy Spark logging configuration
# RUN cp /app/conf/log4j2.properties "${SPARK_HOME}/conf/"

# # Create data directories
# RUN mkdir -p /app/data/models \
#     /app/data/lake \
#     /app/data/export \
#     /app/data/ground_truth \
#     /app/data/reports \
#     /app/data/checkpoints

# # Set Python path
# ENV PYTHONPATH=/app/src:${PYTHONPATH}

# # Default command
# CMD ["bash"]

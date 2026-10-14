"""Spark Structured Streaming entrypoint.

Every 10 seconds Spark reads whatever new GPS, sensor and weather messages
Kafka has, and process_batch turns that micro-batch into congestion scores.
Watch the same numbers in the Spark UI at http://localhost:4040.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, "/app/src")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession

from mapyourway import config
from mapyourway.streaming.process import process_batch


def main() -> None:
    config.ensure_directories()
    spark = (
        SparkSession.builder.appName("mapyourway-mumbai")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", ",".join(config.ALL_INPUT_TOPICS))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    checkpoint = os.path.join(config.CHECKPOINT_DIR, "live-v1")
    query = (
        raw.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=config.TRIGGER_PROCESSING_TIME)
        .start()
    )
    print(f"[spark] listening on {config.KAFKA_BOOTSTRAP} topics {config.ALL_INPUT_TOPICS}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()

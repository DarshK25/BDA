"""Evaluate a saved congestion model on labelled historical data.

The previous version of this module imported a ``generate_dataset`` helper that
does not exist in the real-data training path.  Evaluation now deliberately
accepts a held-out file, which prevents synthetic accuracy from being mistaken
for validation of TomTom data.

Usage::

    python -m mapyourway.ml.evaluate DATA_PATH MODEL_PATH

``DATA_PATH`` must contain the columns accepted by
``mapyourway.ml.train.engineer_features`` and a ``congestion_label`` column is
generated from the observed traffic values in the same way as training.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from mapyourway import config
from mapyourway.ml.train import engineer_features, load_training_data

logger = logging.getLogger(__name__)


def evaluate_model(
    spark,
    data_path: str,
    model_dir: str = config.MODEL_PATH,
    metrics_path: str = config.EVAL_METRICS_JSON,
) -> dict:
    """Evaluate a saved Spark ML pipeline on a labelled data file."""
    from pyspark.ml import PipelineModel
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator

    if not os.path.exists(model_dir):
        logger.error("Model not found at %s", model_dir)
        return {}

    raw = load_training_data(spark, data_path)
    prepared = engineer_features(raw)
    model = PipelineModel.load(model_dir)
    predictions = model.transform(prepared).cache()

    metrics = {
        "n_test": predictions.count(),
        "accuracy": round(
            MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName="accuracy"
            ).evaluate(predictions),
            4,
        ),
        "weighted_f1": round(
            MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName="f1"
            ).evaluate(predictions),
            4,
        ),
        "weighted_precision": round(
            MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
            ).evaluate(predictions),
            4,
        ),
        "weighted_recall": round(
            MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName="weightedRecall"
            ).evaluate(predictions),
            4,
        ),
        "data_path": data_path,
        "note": "evaluated on a held-out file; labels are derived by the training contract",
    }

    confusion = {}
    for row in predictions.groupBy("label", "prediction").count().collect():
        true_key = f"true_{int(row['label'])}"
        confusion.setdefault(true_key, {})[f"pred_{int(row['prediction'])}"] = row["count"]
    metrics["confusion_matrix"] = confusion

    os.makedirs(os.path.dirname(metrics_path) or ".", exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2)

    logger.info("Evaluation metrics written to %s", metrics_path)
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_path", help="Parquet or CSV file containing held-out traffic data")
    parser.add_argument("model_path", nargs="?", default=config.MODEL_PATH)
    parser.add_argument("--metrics-path", default=config.EVAL_METRICS_JSON)
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.appName("mapyourway-evaluate")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        metrics = evaluate_model(spark, args.data_path, args.model_path, args.metrics_path)
        return 0 if metrics else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

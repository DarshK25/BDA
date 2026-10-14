"""Train the congestion classifier used by the live Spark stream.

The model contract is intentionally the same as the streaming feature table:

``speed_ratio, occupancy_pct, veh_per_lane, rain_mm_hr, visibility_km``

The loader accepts either the Parquet output produced by the stream or a CSV /
Parquet historical file containing equivalent raw TomTom fields.  For a useful
model, the training file should contain a real ``congestion_level`` label.  If
it does not, a transparent speed-ratio label is created as a bootstrap only;
that is a rule reproduction, not an independent ML validation result.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import OneVsRest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    greatest,
    lit,
    lower,
    when,
)

from mapyourway import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create the local Spark session used by the training command."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName("MapYourWay-Training")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_training_data(spark: SparkSession, data_path: str) -> DataFrame:
    """Load a Parquet directory/file, or fall back to a typed CSV read."""
    logger.info("Loading training data from %s", data_path)
    try:
        dataframe = spark.read.parquet(data_path)
    except Exception:
        logger.info("Parquet read failed; trying CSV at %s", data_path)
        dataframe = spark.read.csv(data_path, header=True, inferSchema=True)
    logger.info("Loaded %s records", dataframe.count())
    return dataframe


def _first_existing(dataframe: DataFrame, names: list[str], default: float):
    expressions = [col(name) for name in names if name in dataframe.columns]
    return coalesce(*expressions, lit(default)) if expressions else lit(default)


def engineer_features(dataframe: DataFrame) -> DataFrame:
    """Normalize supported raw/lake columns to the live feature contract."""
    df = dataframe
    current_speed = _first_existing(
        df, ["current_speed_kmph", "current_speed", "mean_speed"], 0.0
    )
    free_speed = _first_existing(
        df, ["free_flow_kmph", "free_flow_speed", "free_flow_speed_kmph"], 1.0
    )
    if "speed_ratio" in df.columns:
        speed_ratio = col("speed_ratio")
    else:
        speed_ratio = when(
            free_speed > 0, current_speed / greatest(free_speed, lit(1.0))
        ).otherwise(1.0)

    occupancy = _first_existing(
        df, ["occupancy_pct", "occupancy", "avg_occupancy", "mean_occupancy"], 0.0
    )
    # Historical pandas/R files sometimes store occupancy as 0..1.  The live
    # contract is always percentage points.
    occupancy = when(occupancy <= 1.0, occupancy * 100.0).otherwise(occupancy)
    if "veh_per_lane" in df.columns:
        veh_per_lane = col("veh_per_lane")
    elif "flow_per_lane" in df.columns and not {
        "vehicle_count", "mean_vehicle_count"
    }.intersection(df.columns):
        veh_per_lane = col("flow_per_lane")
    else:
        vehicle_count = _first_existing(
            df, ["vehicle_count", "mean_vehicle_count"], 0.0
        )
        lanes = _first_existing(df, ["lanes"], 1.0)
        # The fallback must not discard a valid flow_per_lane column simply
        # because a nullable vehicle_count column also exists.
        veh_per_lane = when(
            lanes > 0, vehicle_count / greatest(lanes, lit(1.0))
        ).otherwise(0.0)
    rain = _first_existing(df, ["rain_mm_hr", "rain", "mean_rain"], 0.0)
    visibility = _first_existing(
        df, ["visibility_km", "mean_visibility_km", "visibility"], 10.0
    )
    # Older files used metres for a column named visibility.  Current data is
    # always in kilometres, so only convert values that clearly look like m.
    if "visibility" in df.columns and "visibility_km" not in df.columns:
        visibility = when(visibility > 100, visibility / 1000.0).otherwise(visibility)

    df = (
        df.withColumn("speed_ratio", speed_ratio)
        .withColumn("occupancy_pct", occupancy)
        .withColumn("veh_per_lane", veh_per_lane)
        .withColumn("rain_mm_hr", rain)
        .withColumn("visibility_km", visibility)
    )

    if "label_source" in df.columns:
        label_source = col("label_source")
    elif "ground_truth_source" in df.columns:
        label_source = col("ground_truth_source")
    elif "prediction_source" in df.columns:
        # Spark predictions are serving outputs, not ground truth.  Never
        # silently promote them to labels when training from the lake.
        label_source = lit("live_prediction")
    else:
        label_source = lit("provided")

    if "congestion_level" in df.columns:
        label = col("congestion_level").cast("double")
    elif "label" in df.columns:
        label = col("label").cast("double")
    elif "congestion_label" in df.columns:
        label = (
            when(col("congestion_label") == "free_flow", 0.0)
            .when(col("congestion_label") == "moderate", 1.0)
            .when(col("congestion_label") == "heavy", 2.0)
            .when(col("congestion_label") == "severe", 3.0)
        )
        label_source = lit("provided")
    else:
        # Bootstrap fallback only.  This keeps the pipeline runnable before a
        # labelled historical dataset exists, but is explicitly not a model
        # quality claim.
        label = (
            when(col("speed_ratio") >= 0.8, 0.0)
            .when(col("speed_ratio") >= 0.5, 1.0)
            .when(col("speed_ratio") >= 0.3, 2.0)
            .otherwise(3.0)
        )
        label_source = lit("derived_speed_rule")

    return df.withColumn("label", label).withColumn("label_source", label_source)


def train_classifier(
    dataframe: DataFrame,
    model_output_path: str,
    metrics_path: str = config.METRICS_JSON,
    allow_derived_labels: bool = False,
    algorithm: str = "random_forest",
):
    """Fit and save a classifier with the live feature contract.

    ``gbt`` uses Spark's binary GBT classifier inside One-vs-Rest so it can
    predict all four congestion classes.  RandomForest remains the default
    serving model because it is natively multiclass and is faster to start.
    """
    feature_cols = config.FEATURE_COLUMNS
    prepared = engineer_features(dataframe).dropna(
        subset=feature_cols + ["label"]
    ).cache()
    label_sources = [
        row["label_source"]
        for row in prepared.select("label_source").distinct().collect()
    ]
    if any(source != "provided" for source in label_sources) and not allow_derived_labels:
        raise ValueError(
            "Training data contains derived labels. Supply --allow-derived-labels "
            "only for a bootstrap model; independent ground truth is required "
            "for production evaluation."
        )
    class_count = prepared.select("label").distinct().count()
    if class_count < 2:
        raise ValueError("Training data must contain at least two congestion classes")
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )
    if algorithm == "random_forest":
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=config.ML_CONFIG["rf_n_estimators"],
            maxDepth=config.ML_CONFIG["rf_max_depth"],
            minInstancesPerNode=1,
            seed=config.ML_CONFIG["random_state"],
        )
    elif algorithm == "gbt":
        binary_gbt = GBTClassifier(
            featuresCol="features",
            labelCol="label",
            maxIter=config.ML_CONFIG["gbt_n_estimators"],
            maxDepth=config.ML_CONFIG["gbt_max_depth"],
            stepSize=config.ML_CONFIG["gbt_learning_rate"],
            seed=config.ML_CONFIG["random_state"],
        )
        classifier = OneVsRest(
            featuresCol="features", labelCol="label", classifier=binary_gbt
        )
    else:
        raise ValueError("algorithm must be 'random_forest' or 'gbt'")
    pipeline = Pipeline(stages=[assembler, classifier])
    row_count = prepared.count()
    if "timestamp" in prepared.columns:
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        order_window = Window.orderBy(col("timestamp"), col("segment_id"))
        ordered = prepared.withColumn("_training_row", row_number().over(order_window))
        train_count = max(1, int(row_count * (1.0 - config.ML_CONFIG["test_size"])))
        train_df = ordered.where(col("_training_row") <= train_count).drop("_training_row")
        test_df = ordered.where(col("_training_row") > train_count).drop("_training_row")
    else:
        train_df, test_df = prepared.randomSplit(
            [1.0 - config.ML_CONFIG["test_size"], config.ML_CONFIG["test_size"]],
            seed=config.ML_CONFIG["random_state"],
        )
    logger.info("Training set: %s records", train_df.count())
    logger.info("Test set: %s records", test_df.count())
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df).cache()

    def metric(name: str) -> float:
        return round(
            MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName=name
            ).evaluate(predictions),
            4,
        )

    metrics = {
        "n_train": train_df.count(),
        "n_test": test_df.count(),
        "accuracy": metric("accuracy"),
        "weighted_f1": metric("f1"),
        "weighted_precision": metric("weightedPrecision"),
        "weighted_recall": metric("weightedRecall"),
        "feature_columns": feature_cols,
        "label_sources": label_sources,
        "split": "chronological" if "timestamp" in prepared.columns else "random",
        "note": "Only independently labelled rows support a production quality claim; derived or live-prediction labels are bootstrap calibration.",
        "model_input_contract": "speed_ratio, occupancy_pct, veh_per_lane, rain_mm_hr, visibility_km",
        "algorithm": algorithm,
    }
    os.makedirs(os.path.dirname(model_output_path) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(metrics_path) or ".", exist_ok=True)
    model.write().overwrite().save(model_output_path)
    with open(metrics_path, "w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2)
    logger.info("Model saved to %s", model_output_path)
    logger.info("Metrics saved to %s", metrics_path)
    prepared.unpersist()
    return model, metrics


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_path", help="Historical Parquet directory or CSV file")
    parser.add_argument("model_path", nargs="?", default=config.MODEL_PATH)
    parser.add_argument("--metrics-path", default=config.METRICS_JSON)
    parser.add_argument(
        "--algorithm",
        choices=("random_forest", "gbt"),
        default="random_forest",
        help="Classifier to train; random_forest is the live serving default.",
    )
    parser.add_argument(
        "--allow-derived-labels",
        action="store_true",
        help="Train a bootstrap model from the speed-ratio rule; not a quality claim.",
    )
    args = parser.parse_args()

    spark = create_spark_session()
    try:
        raw = load_training_data(spark, args.data_path)
        _, metrics = train_classifier(
            raw,
            args.model_path,
            args.metrics_path,
            allow_derived_labels=args.allow_derived_labels,
            algorithm=args.algorithm,
        )
        logger.info("Training complete: %s", metrics)
        return 0
    except Exception:
        logger.exception("Training failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

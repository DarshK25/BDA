"""Train METR-LA model using scikit-learn (faster than Spark for single machine)

This trains a real traffic model on METR-LA data using sklearn.
Much faster than Spark for datasets that fit in memory.
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score, f1_score, precision_score, recall_score,
    classification_report, confusion_matrix
)

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from mapyourway import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_metrla_csv(csv_path: str) -> pd.DataFrame:
    """Load METR-LA CSV and convert to long format"""
    logger.info(f"Loading METR-LA data from {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # First column is timestamp
    timestamp_col = df.columns[0]
    sensor_cols = [c for c in df.columns if c != timestamp_col]
    
    logger.info(f"Found {len(sensor_cols)} sensors, {len(df)} timesteps")
    
    # Convert to long format
    df_long = df.melt(
        id_vars=[timestamp_col],
        value_vars=sensor_cols,
        var_name='sensor_id',
        value_name='speed_mph'
    )
    
    # Convert timestamp
    df_long['timestamp'] = pd.to_datetime(df_long[timestamp_col])
    df_long = df_long.drop(columns=[timestamp_col])
    
    # Filter out zeros (missing data)
    df_long = df_long[df_long['speed_mph'] > 0]
    
    logger.info(f"After filtering: {len(df_long):,} valid readings")
    
    return df_long


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Engineer features to match MapYourWay 5-feature contract"""
    logger.info("Engineering features...")
    
    # Calculate free-flow speed per sensor (95th percentile)
    free_flow = df.groupby('sensor_id')['speed_mph'].quantile(0.95)
    df = df.merge(free_flow.rename('free_flow_speed'), on='sensor_id', how='left')
    
    # Speed ratio
    df['speed_ratio'] = (df['speed_mph'] / df['free_flow_speed'].clip(lower=1.0)).clip(0, 1.3)
    
    # Occupancy (Greenshields relationship)
    df['occupancy_pct'] = ((1.0 - df['speed_ratio']) * 120.0).clip(0, 100)
    
    # Vehicle density
    df['veh_per_lane'] = (3.0 + 9.0 * df['occupancy_pct'] / 100.0).clip(0, None)
    
    # Weather (no actual data, set defaults)
    df['rain_mm_hr'] = 0.0
    df['visibility_km'] = 10.0
    
    # Temporal features
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    
    # Congestion level labels
    df['congestion_level'] = pd.cut(
        df['speed_ratio'],
        bins=[0, 0.3, 0.5, 0.8, 2.0],
        labels=[3, 2, 1, 0],  # SEVERE, HEAVY, MODERATE, FREE_FLOW
        include_lowest=True
    ).astype(float)
    
    logger.info("Feature engineering complete")
    
    return df


def train_model(
    df: pd.DataFrame,
    model_path: str,
    metrics_path: str,
    algorithm: str = "random_forest"
):
    """Train sklearn model"""
    
    feature_cols = config.FEATURE_COLUMNS
    
    # Sort by timestamp for chronological split
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # 70% train, 15% val, 15% test
    n = len(df)
    train_end = int(n * 0.70)
    val_end = int(n * 0.85)
    
    train_df = df.iloc[:train_end]
    val_df = df.iloc[train_end:val_end]
    test_df = df.iloc[val_end:]
    
    logger.info(f"Train: {len(train_df):,} | Val: {len(val_df):,} | Test: {len(test_df):,}")
    
    # Class distribution
    logger.info("\nClass distribution (Test set):")
    level_names = {0: 'FREE_FLOW', 1: 'MODERATE', 2: 'HEAVY', 3: 'SEVERE'}
    for level, count in test_df['congestion_level'].value_counts().sort_index().items():
        pct = count / len(test_df) * 100
        logger.info(f"  {level_names[level]:12s}: {count:6,} ({pct:5.2f}%)")
    
    # Prepare features and labels
    X_train = train_df[feature_cols].values
    y_train = train_df['congestion_level'].values
    
    X_test = test_df[feature_cols].values
    y_test = test_df['congestion_level'].values
    
    # Train model
    logger.info(f"\nTraining {algorithm} model...")
    
    if algorithm == "random_forest":
        model = RandomForestClassifier(
            n_estimators=config.ML_CONFIG["rf_n_estimators"],
            max_depth=config.ML_CONFIG["rf_max_depth"],
            min_samples_split=config.ML_CONFIG["rf_min_samples_split"],
            random_state=config.ML_CONFIG["random_state"],
            n_jobs=-1,
            verbose=1
        )
    elif algorithm == "gbt":
        model = GradientBoostingClassifier(
            n_estimators=config.ML_CONFIG["gbt_n_estimators"],
            max_depth=config.ML_CONFIG["gbt_max_depth"],
            learning_rate=config.ML_CONFIG["gbt_learning_rate"],
            random_state=config.ML_CONFIG["random_state"],
            verbose=1
        )
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
    
    model.fit(X_train, y_train)
    
    # Evaluate
    logger.info("\nEvaluating on test set...")
    y_pred = model.predict(X_test)
    
    metrics = {
        "dataset": "METR-LA",
        "description": "Real loop-detector traffic data from Los Angeles highways",
        "n_train": int(len(train_df)),
        "n_val": int(len(val_df)),
        "n_test": int(len(test_df)),
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "weighted_f1": float(f1_score(y_test, y_pred, average='weighted')),
        "weighted_precision": float(precision_score(y_test, y_pred, average='weighted')),
        "weighted_recall": float(recall_score(y_test, y_pred, average='weighted')),
        "feature_columns": feature_cols,
        "label_sources": ["metrla_real_data"],
        "split": "chronological",
        "algorithm": algorithm,
        "note": "Trained on REAL traffic data from 207 LA highway sensors over 118 days",
        "model_input_contract": "speed_ratio, occupancy_pct, veh_per_lane, rain_mm_hr, visibility_km",
        "trained_date": datetime.now().isoformat(),
        "sklearn_version": True
    }
    
    # Print results
    logger.info(f"\n{'='*60}")
    logger.info("TEST SET RESULTS")
    logger.info(f"{'='*60}")
    logger.info(f"Accuracy:  {metrics['accuracy']:.4f}")
    logger.info(f"F1 Score:  {metrics['weighted_f1']:.4f}")
    logger.info(f"Precision: {metrics['weighted_precision']:.4f}")
    logger.info(f"Recall:    {metrics['weighted_recall']:.4f}")
    
    logger.info(f"\n{classification_report(y_test, y_pred, target_names=['FREE_FLOW', 'MODERATE', 'HEAVY', 'SEVERE'])}")
    
    logger.info("\nConfusion Matrix:")
    cm = confusion_matrix(y_test, y_pred)
    logger.info(f"\n{cm}")
    
    # Feature importance
    if hasattr(model, 'feature_importances_'):
        logger.info("\nFeature Importances:")
        for feat, imp in sorted(zip(feature_cols, model.feature_importances_), 
                                key=lambda x: x[1], reverse=True):
            logger.info(f"  {feat:20s}: {imp:.4f}")
        
        metrics["feature_importances"] = {
            feat: float(imp) for feat, imp in zip(feature_cols, model.feature_importances_)
        }
    
    # Save model and metrics
    Path(model_path).parent.mkdir(parents=True, exist_ok=True)
    Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)
    
    joblib.dump(model, model_path)
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    logger.info(f"\nModel saved to: {model_path}")
    logger.info(f"Metrics saved to: {metrics_path}")
    
    return model, metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", help="Path to METR-LA.csv")
    parser.add_argument("--model-path", default="data/models/metrla_sklearn_model.joblib")
    parser.add_argument("--metrics-path", default="data/models/metrla_sklearn_metrics.json")
    parser.add_argument(
        "--algorithm",
        choices=["random_forest", "gbt"],
        default="random_forest"
    )
    args = parser.parse_args()
    
    try:
        # Load data
        df = load_metrla_csv(args.csv_path)
        
        # Engineer features
        df = engineer_features(df)
        
        # Train model
        model, metrics = train_model(
            df,
            args.model_path,
            args.metrics_path,
            algorithm=args.algorithm
        )
        
        logger.info("\n✅ Training complete!")
        logger.info(f"Test Accuracy: {metrics['accuracy']:.4f}")
        
        return 0
        
    except Exception as e:
        logger.exception("Training failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())

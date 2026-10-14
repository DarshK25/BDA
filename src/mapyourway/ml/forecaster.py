"""Load the trained RandomForest model and predict congestion levels from feature dicts.

This is the inference companion to train.py. The streaming job uses this to classify
each 30-second window after computing features.
"""
from __future__ import annotations

import os
from typing import TYPE_CHECKING

from mapyourway import config

if TYPE_CHECKING:
    from pyspark.ml import PipelineModel


class CongestionForecaster:
    """Wraps the PySpark MLlib RandomForest pipeline for batch prediction."""

    def __init__(self, model_dir: str = config.MODEL_PATH):
        from pyspark.sql import SparkSession

        self.spark = SparkSession.getActiveSession()
        if self.spark is None:
            raise RuntimeError("No active SparkSession. Create one before instantiating CongestionForecaster.")
        
        if not os.path.exists(model_dir):
            raise FileNotFoundError(
                f"Model directory not found: {model_dir}\n"
                f"Run 'python -m mapyourway.ml.train' first to train the model."
            )
        
        from pyspark.ml import PipelineModel
        self.model: PipelineModel = PipelineModel.load(model_dir)
        print(f"[forecaster] loaded model from {model_dir}", flush=True)

    def predict_batch(self, feature_dicts: list[dict]) -> list[dict]:
        """
        Predict congestion levels for a batch of feature dictionaries.
        
        Args:
            feature_dicts: list of dicts with keys matching config.FEATURES
        
        Returns:
            list of dicts with original features plus 'prediction' and 'probability' keys
        """
        if not feature_dicts:
            return []
        
        import pandas as pd
        
        # Convert to Spark DataFrame
        pdf = pd.DataFrame(feature_dicts)
        df = self.spark.createDataFrame(pdf)
        
        # Predict
        pred_df = self.model.transform(df)
        
        # Convert back to list of dicts
        result_pdf = pred_df.toPandas()
        results = []
        for _, row in result_pdf.iterrows():
            result = row.to_dict()
            # Extract prediction and probability
            result['prediction'] = int(result.get('prediction', 0))
            # Probability is a DenseVector, convert to list
            if 'probability' in result:
                prob_vector = result['probability']
                if hasattr(prob_vector, 'toArray'):
                    result['probability'] = prob_vector.toArray().tolist()
                elif hasattr(prob_vector, '__iter__'):
                    result['probability'] = list(prob_vector)
            results.append(result)
        
        return results

    def predict_single(self, feature_dict: dict) -> dict:
        """Convenience method for single prediction."""
        results = self.predict_batch([feature_dict])
        return results[0] if results else {}


def load_forecaster(model_dir: str = config.MODEL_PATH) -> CongestionForecaster:
    """Factory function to create a forecaster instance."""
    return CongestionForecaster(model_dir=model_dir)

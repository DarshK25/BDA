"""
Feature engineering for ML models.
Transforms raw averages into the 5 model input features.
"""

import math
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple


def extract_time_features(timestamp: datetime) -> Dict[str, float]:
    """
    Extract cyclical time features from timestamp.
    
    Args:
        timestamp: DateTime object
    
    Returns:
        Dict with sin/cos encoded time features
    """
    hour = timestamp.hour + timestamp.minute / 60.0
    
    # Encode hour as cyclical feature (0-24 -> 0-2π)
    hour_rad = (hour / 24.0) * 2 * math.pi
    
    return {
        "time_of_day_sin": math.sin(hour_rad),
        "time_of_day_cos": math.cos(hour_rad),
        "hour": timestamp.hour,
        "day_of_week": timestamp.weekday(),
        "is_weekend": 1 if timestamp.weekday() >= 5 else 0
    }


def calculate_rolling_features(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """
    Calculate rolling statistics for time-series features.
    
    Args:
        df: DataFrame with traffic data
        window: Rolling window size
    
    Returns:
        DataFrame with added rolling features
    """
    df = df.sort_values("timestamp")
    
    # Rolling averages
    df["speed_rolling_avg"] = df.groupby("road_id")["avg_speed"].transform(
        lambda x: x.rolling(window, min_periods=1).mean()
    )
    
    df["occupancy_rolling_avg"] = df.groupby("road_id")["avg_occupancy"].transform(
        lambda x: x.rolling(window, min_periods=1).mean()
    )
    
    # Rolling standard deviation
    df["speed_rolling_std"] = df.groupby("road_id")["avg_speed"].transform(
        lambda x: x.rolling(window, min_periods=1).std()
    ).fillna(0)
    
    return df


def create_lag_features(df: pd.DataFrame, lags: List[int] = [1, 2, 3]) -> pd.DataFrame:
    """
    Create lagged features for time-series prediction.
    
    Args:
        df: DataFrame with traffic data
        lags: List of lag periods
    
    Returns:
        DataFrame with lagged features
    """
    df = df.sort_values("timestamp")
    
    for lag in lags:
        df[f"speed_lag_{lag}"] = df.groupby("road_id")["avg_speed"].shift(lag)
        df[f"occupancy_lag_{lag}"] = df.groupby("road_id")["avg_occupancy"].shift(lag)
        df[f"congestion_lag_{lag}"] = df.groupby("road_id")["congestion_level"].shift(lag)
    
    # Missing history belongs to this road, never to the previous road's row.
    lag_columns = [c for c in df.columns if "_lag_" in c]
    df[lag_columns] = df[lag_columns].fillna(0)
    
    return df


def engineer_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create interaction features between variables.
    
    Args:
        df: DataFrame with basic features
    
    Returns:
        DataFrame with interaction features
    """
    # Speed × Occupancy interaction
    df["speed_occupancy_interaction"] = df["avg_speed"] * df["avg_occupancy"]
    
    # Normalized vehicle count
    if "capacity" in df.columns:
        df["vehicle_density"] = df["vehicle_count"] / df["capacity"]
    
    # Speed deviation from speed limit
    if "speed_limit" in df.columns:
        df["speed_deviation"] = df["speed_limit"] - df["avg_speed"]
        df["speed_ratio"] = df["avg_speed"] / df["speed_limit"]
    
    return df


def create_model_features(raw_data: pd.DataFrame) -> pd.DataFrame:
    """
    Main feature engineering pipeline.
    Transforms raw data into the canonical five live-stream features plus
    additional exploratory features.
    
    Expected raw_data columns:
        - timestamp
        - road_id
        - avg_speed
        - avg_occupancy
        - vehicle_count
    
    Returns:
        DataFrame with engineered features
    """
    df = raw_data.copy()
    
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    if "road_id" not in df.columns:
        df["road_id"] = df.get("segment_id", "all")
    if "avg_speed" not in df.columns:
        df["avg_speed"] = df.get("current_speed_kmph", df.get("current_speed", 0.0))
    if "avg_occupancy" not in df.columns:
        occupancy = df.get("occupancy_pct", 0.0)
        df["avg_occupancy"] = occupancy / 100.0 if hasattr(occupancy, "__truediv__") else occupancy
    
    # Extract time features
    time_features = df["timestamp"].apply(extract_time_features)
    time_df = pd.DataFrame(time_features.tolist(), index=df.index)
    df = pd.concat([df, time_df], axis=1)
    
    # Calculate rolling features
    df = calculate_rolling_features(df, window=3)
    
    # Create interaction features
    df = engineer_interaction_features(df)
    
    # Keep the Python batch feature path aligned with the Spark path and
    # ml/train.py.  Existing notebooks may still provide the older avg_*
    # names, so normalize those names here rather than silently producing a
    # different model input vector.
    if "speed_ratio" not in df.columns:
        if {"avg_speed", "free_flow_kmph"}.issubset(df.columns):
            df["speed_ratio"] = (
                df["avg_speed"] / df["free_flow_kmph"].replace(0, np.nan)
            ).fillna(1.0)
        elif {"avg_speed", "free_flow_speed_kmph"}.issubset(df.columns):
            df["speed_ratio"] = (
                df["avg_speed"] / df["free_flow_speed_kmph"].replace(0, np.nan)
            ).fillna(1.0)
        else:
            df["speed_ratio"] = 1.0

    if "occupancy_pct" not in df.columns:
        if "avg_occupancy" in df.columns:
            occupancy = df["avg_occupancy"]
            df["occupancy_pct"] = occupancy * 100.0 if occupancy.max() <= 1 else occupancy
        else:
            df["occupancy_pct"] = 0.0

    if "veh_per_lane" not in df.columns:
        if "flow_per_lane" in df.columns:
            df["veh_per_lane"] = df["flow_per_lane"]
        elif "vehicle_count" in df.columns and "lanes" in df.columns:
            df["veh_per_lane"] = df["vehicle_count"] / df["lanes"].replace(0, np.nan)
        elif "vehicle_count" in df.columns:
            df["veh_per_lane"] = df["vehicle_count"]
        else:
            df["veh_per_lane"] = 0.0

    if "rain_mm_hr" not in df.columns:
        df["rain_mm_hr"] = df.get("rain", 0.0)
    if "visibility_km" not in df.columns:
        df["visibility_km"] = df.get("visibility", 10.0)
    if "visibility" in df.columns and "visibility_km" not in raw_data.columns:
        df["visibility_km"] = df["visibility_km"].where(
            df["visibility_km"] <= 100, df["visibility_km"] / 1000.0
        )

    feature_columns = [
        "speed_ratio",
        "occupancy_pct",
        "veh_per_lane",
        "rain_mm_hr",
        "visibility_km",
    ]
    for column in feature_columns:
        df[column] = df[column].fillna(0.0)

    return df


def normalize_features(
    df: pd.DataFrame, feature_cols: List[str]
) -> Tuple[pd.DataFrame, Dict]:
    """
    Normalize features using min-max scaling.
    
    Args:
        df: DataFrame with features
        feature_cols: List of columns to normalize
    
    Returns:
        Tuple of (normalized DataFrame, normalization parameters)
    """
    normalization_params = {}
    df_normalized = df.copy()
    
    for col in feature_cols:
        if col in df.columns:
            min_val = df[col].min()
            max_val = df[col].max()
            
            if max_val - min_val > 0:
                df_normalized[col] = (df[col] - min_val) / (max_val - min_val)
            else:
                df_normalized[col] = 0.0
            
            normalization_params[col] = {"min": min_val, "max": max_val}
    
    return df_normalized, normalization_params


def denormalize_features(
    df: pd.DataFrame,
    feature_cols: List[str],
    normalization_params: Dict,
) -> pd.DataFrame:
    """
    Reverse normalization using stored parameters.
    
    Args:
        df: DataFrame with normalized features
        feature_cols: List of columns to denormalize
        normalization_params: Dict with min/max values
    
    Returns:
        DataFrame with original scale
    """
    df_denormalized = df.copy()
    
    for col in feature_cols:
        if col in df.columns and col in normalization_params:
            params = normalization_params[col]
            min_val = params["min"]
            max_val = params["max"]
            
            if max_val - min_val > 0:
                df_denormalized[col] = df[col] * (max_val - min_val) + min_val
    
    return df_denormalized


if __name__ == "__main__":
    # Demo feature engineering
    from datetime import datetime, timedelta
    
    # Create sample data
    timestamps = [
        datetime.now() - timedelta(minutes=10 * i) for i in range(10, 0, -1)
    ]
    
    sample_data = pd.DataFrame({
        "timestamp": timestamps,
        "road_id": ["R1"] * 10,
        "avg_speed": [50, 45, 40, 38, 35, 32, 30, 28, 26, 25],
        "avg_occupancy": [0.3, 0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85],
        "vehicle_count": [15, 20, 25, 28, 30, 33, 35, 38, 40, 42]
    })
    
    print("Original Data:")
    print(sample_data[["timestamp", "avg_speed", "avg_occupancy"]].head())
    
    # Engineer features
    features = create_model_features(sample_data)
    
    print("\nEngineered Features:")
    print(
        features[
            [
                "avg_speed",
                "avg_occupancy",
                "vehicle_count",
                "time_of_day_sin",
                "time_of_day_cos",
            ]
        ].head()
    )
    
    print("\nAdditional Features:")
    print(features[["speed_rolling_avg", "speed_rolling_std"]].head())

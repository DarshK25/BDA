"""
Complete EDA for METR-LA Traffic Dataset
Analyzes real loop-detector traffic data from Los Angeles
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json

def perform_eda(csv_path: str, output_dir: str = "data/reports/metrla_eda"):
    """Complete EDA on METR-LA dataset"""
    
    print("="*80)
    print("METR-LA DATASET - COMPREHENSIVE EDA")
    print("="*80)
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 1. LOAD DATA
    print("\n[1/8] Loading dataset...")
    df = pd.read_csv(csv_path)
    
    # Check if first column is timestamp
    first_col = df.columns[0]
    timestamps = pd.to_datetime(df.iloc[:, 0], errors='coerce')
    
    if timestamps.notna().mean() > 0.9:
        print(f"   ✓ First column '{first_col}' contains timestamps")
        df.index = timestamps
        speed_data = df.iloc[:, 1:]
    else:
        print(f"   ✓ No timestamp column found, using sequential index")
        print(f"   ℹ Assuming 5-minute intervals starting 2012-03-01")
        df.index = pd.date_range('2012-03-01', periods=len(df), freq='5min')
        speed_data = df
    
    # Convert to numeric
    speed_data = speed_data.apply(pd.to_numeric, errors='coerce')
    
    # 2. BASIC STATISTICS
    print("\n[2/8] Dataset Overview")
    print(f"   Time Range: {df.index.min()} to {df.index.max()}")
    print(f"   Duration: {(df.index.max() - df.index.min()).days} days")
    print(f"   Total Timesteps: {len(df):,}")
    print(f"   Number of Sensors: {len(speed_data.columns)}")
    print(f"   Total Data Points: {speed_data.size:,}")
    print(f"   Resolution: 5-minute intervals")
    
    # 3. DATA QUALITY
    print("\n[3/8] Data Quality Assessment")
    
    # Missing values
    missing_pct = (speed_data.isna().sum() / len(speed_data) * 100)
    zero_pct = ((speed_data == 0).sum() / len(speed_data) * 100)
    
    print(f"   Missing Values: {speed_data.isna().sum().sum():,} ({speed_data.isna().mean().mean()*100:.2f}%)")
    print(f"   Zero Values: {(speed_data == 0).sum().sum():,} ({((speed_data == 0).sum().sum() / speed_data.size)*100:.2f}%)")
    print(f"   Valid Readings: {speed_data.notna().sum().sum():,} ({speed_data.notna().mean().mean()*100:.2f}%)")
    
    print(f"\n   Sensors by Data Completeness:")
    completeness = (1 - missing_pct - zero_pct)
    print(f"   - Excellent (>95%): {(completeness > 95).sum()} sensors")
    print(f"   - Good (90-95%): {((completeness >= 90) & (completeness <= 95)).sum()} sensors")
    print(f"   - Fair (80-90%): {((completeness >= 80) & (completeness < 90)).sum()} sensors")
    print(f"   - Poor (<80%): {(completeness < 80).sum()} sensors")
    
    # 4. SPEED STATISTICS
    print("\n[4/8] Speed Distribution Analysis")
    
    # Replace zeros with NaN for statistics (0 = no data in METR-LA)
    speed_clean = speed_data.replace(0, np.nan)
    
    print(f"   Overall Statistics (mph):")
    print(f"   - Mean Speed: {speed_clean.mean().mean():.2f} mph")
    print(f"   - Median Speed: {speed_clean.median().median():.2f} mph")
    print(f"   - Std Dev: {speed_clean.std().mean():.2f} mph")
    print(f"   - Min Speed: {speed_clean.min().min():.2f} mph")
    print(f"   - Max Speed: {speed_clean.max().max():.2f} mph")
    
    # Speed ranges
    all_speeds = speed_clean.values.flatten()
    all_speeds = all_speeds[~np.isnan(all_speeds)]
    
    print(f"\n   Speed Range Distribution:")
    print(f"   - 0-20 mph (Severe): {((all_speeds < 20).sum() / len(all_speeds) * 100):.1f}%")
    print(f"   - 20-40 mph (Heavy): {(((all_speeds >= 20) & (all_speeds < 40)).sum() / len(all_speeds) * 100):.1f}%")
    print(f"   - 40-60 mph (Moderate): {(((all_speeds >= 40) & (all_speeds < 60)).sum() / len(all_speeds) * 100):.1f}%")
    print(f"   - 60+ mph (Free Flow): {((all_speeds >= 60).sum() / len(all_speeds) * 100):.1f}%")
    
    # 5. TEMPORAL PATTERNS
    print("\n[5/8] Temporal Pattern Analysis")
    
    df['hour'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek
    df['is_weekend'] = df['day_of_week'] >= 5
    
    hourly_mean = speed_clean.groupby(df.index.hour).mean().mean(axis=1)
    print(f"\n   Hourly Speed Patterns:")
    print(f"   - Peak AM (6-9 AM): {hourly_mean[6:9].mean():.2f} mph")
    print(f"   - Midday (10 AM-2 PM): {hourly_mean[10:14].mean():.2f} mph")
    print(f"   - Peak PM (4-7 PM): {hourly_mean[16:19].mean():.2f} mph")
    print(f"   - Night (10 PM-5 AM): {hourly_mean[[22,23,0,1,2,3,4,5]].mean():.2f} mph")
    
    weekday_mean = speed_clean[~df['is_weekend']].mean().mean()
    weekend_mean = speed_clean[df['is_weekend']].mean().mean()
    print(f"\n   Weekday vs Weekend:")
    print(f"   - Weekday Average: {weekday_mean:.2f} mph")
    print(f"   - Weekend Average: {weekend_mean:.2f} mph")
    print(f"   - Difference: {weekend_mean - weekday_mean:+.2f} mph ({((weekend_mean/weekday_mean - 1)*100):+.1f}%)")
    
    # 6. SENSOR-LEVEL ANALYSIS
    print("\n[6/8] Sensor-Level Statistics")
    
    sensor_stats = pd.DataFrame({
        'mean_speed': speed_clean.mean(),
        'std_speed': speed_clean.std(),
        'completeness': (1 - missing_pct - zero_pct),
        'peak_speed': speed_clean.max(),
        'low_speed': speed_clean.min()
    })
    
    print(f"\n   Top 5 Fastest Sensors (avg speed):")
    for i, (sensor, row) in enumerate(sensor_stats.nlargest(5, 'mean_speed').iterrows(), 1):
        print(f"   {i}. {sensor}: {row['mean_speed']:.1f} mph (±{row['std_speed']:.1f})")
    
    print(f"\n   Top 5 Slowest Sensors (avg speed):")
    for i, (sensor, row) in enumerate(sensor_stats.nsmallest(5, 'mean_speed').iterrows(), 1):
        print(f"   {i}. {sensor}: {row['mean_speed']:.1f} mph (±{row['std_speed']:.1f})")
    
    # 7. CONGESTION ANALYSIS
    print("\n[7/8] Congestion Pattern Analysis")
    
    # Calculate congestion based on speed ratios
    free_flow_speeds = speed_clean.quantile(0.95)
    speed_ratios = speed_clean / free_flow_speeds
    
    congestion_levels = speed_ratios.apply(lambda col: pd.cut(
        col, bins=[0, 0.3, 0.5, 0.8, 2.0],
        labels=['SEVERE', 'HEAVY', 'MODERATE', 'FREE']
    ))
    
    level_counts = congestion_levels.apply(lambda x: x.value_counts()).sum(axis=1)
    level_pcts = (level_counts / level_counts.sum() * 100)
    
    print(f"\n   Overall Congestion Distribution:")
    for level in ['FREE', 'MODERATE', 'HEAVY', 'SEVERE']:
        if level in level_pcts.index:
            print(f"   - {level:12s}: {level_pcts[level]:5.2f}%")
    
    # Peak hour congestion
    am_peak_congestion = congestion_levels[(df.index.hour >= 6) & (df.index.hour <= 9)]
    pm_peak_congestion = congestion_levels[(df.index.hour >= 16) & (df.index.hour <= 19)]
    
    print(f"\n   AM Peak (6-9 AM) Congestion:")
    am_counts = am_peak_congestion.apply(lambda x: x.value_counts()).sum(axis=1)
    am_pcts = (am_counts / am_counts.sum() * 100)
    for level in ['FREE', 'MODERATE', 'HEAVY', 'SEVERE']:
        if level in am_pcts.index:
            print(f"   - {level:12s}: {am_pcts[level]:5.2f}%")
    
    print(f"\n   PM Peak (4-7 PM) Congestion:")
    pm_counts = pm_peak_congestion.apply(lambda x: x.value_counts()).sum(axis=1)
    pm_pcts = (pm_counts / pm_counts.sum() * 100)
    for level in ['FREE', 'MODERATE', 'HEAVY', 'SEVERE']:
        if level in pm_pcts.index:
            print(f"   - {level:12s}: {pm_pcts[level]:5.2f}%")
    
    # 8. SAVE SUMMARY
    print("\n[8/8] Generating Summary Report")
    
    summary = {
        "dataset": "METR-LA",
        "description": "Real loop-detector traffic speed data from Los Angeles highways",
        "time_range": {
            "start": str(df.index.min()),
            "end": str(df.index.max()),
            "duration_days": int((df.index.max() - df.index.min()).days)
        },
        "dimensions": {
            "timesteps": int(len(df)),
            "sensors": int(len(speed_data.columns)),
            "resolution": "5 minutes"
        },
        "data_quality": {
            "completeness_pct": float(speed_data.notna().mean().mean() * 100),
            "missing_pct": float(speed_data.isna().mean().mean() * 100),
            "zero_pct": float(((speed_data == 0).sum().sum() / speed_data.size) * 100)
        },
        "speed_statistics_mph": {
            "mean": float(speed_clean.mean().mean()),
            "median": float(speed_clean.median().median()),
            "std": float(speed_clean.std().mean()),
            "min": float(speed_clean.min().min()),
            "max": float(speed_clean.max().max())
        },
        "congestion_distribution_pct": {
            level: float(level_pcts[level]) for level in ['FREE', 'MODERATE', 'HEAVY', 'SEVERE'] 
            if level in level_pcts.index
        },
        "temporal_patterns": {
            "weekday_mean_mph": float(weekday_mean),
            "weekend_mean_mph": float(weekend_mean),
            "am_peak_mean_mph": float(hourly_mean[6:9].mean()),
            "pm_peak_mean_mph": float(hourly_mean[16:19].mean())
        }
    }
    
    # Save JSON summary
    summary_path = Path(output_dir) / "metrla_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n   ✓ Summary saved to: {summary_path}")
    
    print("\n" + "="*80)
    print("EDA COMPLETE!")
    print("="*80)
    print(f"\nKey Findings:")
    print(f"• {len(speed_data.columns)} sensors tracking LA highway traffic over {(df.index.max() - df.index.min()).days} days")
    print(f"• Data quality: {speed_data.notna().mean().mean()*100:.1f}% complete")
    print(f"• Average speed: {speed_clean.mean().mean():.1f} mph")
    print(f"• Congestion: {level_pcts.get('HEAVY', 0) + level_pcts.get('SEVERE', 0):.1f}% of time in heavy/severe")
    print(f"• Weekends {weekend_mean - weekday_mean:+.1f} mph faster than weekdays")
    
    return summary, speed_data, df


if __name__ == "__main__":
    import sys
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/METR-LA.csv"
    perform_eda(csv_path)

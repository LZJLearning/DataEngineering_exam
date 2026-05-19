import pandas as pd
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Use relative paths to point to the outer data lake
LAKE_ROOT = "./tmp/datalake"

def get_daily_stats(sensor_type: str, days: int) -> dict:
    """Read the Consumption golden zone to obtain historical statistics."""
    target_dir = f"{LAKE_ROOT}/consumption/use_case=sensor_averages/sensor={sensor_type}"
    if not os.path.exists(target_dir):
        return None
        
    try:
        df = pd.read_parquet(target_dir)
        cutoff_date = datetime.now() - timedelta(days=days)
        df_filtered = df[
            (df['year'] >= cutoff_date.year) & 
            (df['month'] >= cutoff_date.month)
        ]
        
        return {
            "sensor": sensor_type,
            "days_requested": days,
            "total_observations": int(df_filtered['observation_count'].sum()) if not df_filtered.empty else 0,
            "total_anomalies": int(df_filtered['anomaly_count'].sum()) if not df_filtered.empty else 0
        }
    except Exception as e:
        logger.error(f"Lake read error: {e}")
        raise e

def get_recent_anomalies(sensor_type: str, limit: int) -> list:
    """Read the Curated Silver Zone to obtain the most recent exception details."""
    target_dir = f"{LAKE_ROOT}/curated/domain=iot/sensor={sensor_type}"
    if not os.path.exists(target_dir):
        return []
        
    try:
        df = pd.read_parquet(target_dir)
        anomalies_df = df[df['is_anomaly'] == True]
        
        if not anomalies_df.empty:
            anomalies_df = anomalies_df.sort_values(by='event_time', ascending=False).head(limit)
            anomalies_df['event_time'] = anomalies_df['event_time'].astype(str)
            return anomalies_df.to_dict(orient='records')
        return []
    except Exception as e:
        logger.error(f"Lake read error: {e}")
        raise e
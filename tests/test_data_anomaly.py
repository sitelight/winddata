import pandas as pd
import pytest
from data_anomaly import AnomalyDetector

def test_detect_anomalies():
    cleaned_df = pd.DataFrame({
        'turbine_id': [1, 1, 1],
        'timestamp': [
            '2021-01-01 01:00:00', 
            '2021-01-01 02:00:00', 
            '2021-01-01 03:00:00'
        ],
        'power_output': [100, 500, 110] # 500 should be anomaly if std_dev thresholds set tightly
    })

    summary_df = pd.DataFrame({
        'turbine_id': [1],
        'start_timeframe': ['2021-01-01 01:00:00'],
        'end_timeframe': ['2021-01-01 24:00:00'],
        'power_output_average': [105],
        'power_output_std': [10],
        'std_dev_start': [85],
        'std_dev_end': [125],
    })

    detector = AnomalyDetector(cleaned_df, summary_df)
    anomalies_df = detector.detect_anomalies(column='power_output', time_column='timestamp')

    # Check anomalies
    # The second record with power_output=500 should be flagged as anomaly
    assert anomalies_df.loc[anomalies_df['power_output'] == 500, 'anomaly'].iloc[0] == True
    # Others within range 85-125 not anomaly
    assert anomalies_df.loc[anomalies_df['power_output'] == 100, 'anomaly'].iloc[0] == False
    assert anomalies_df.loc[anomalies_df['power_output'] == 110, 'anomaly'].iloc[0] == False
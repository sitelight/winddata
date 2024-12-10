import pandas as pd
from typing import Any


class AnomalyDetector:
    """
    Detects anomalies by comparing each record with summary statistics.
    Flags records whose values fall outside the mean ± (std_dev_multiplier * std).
    """

    def __init__(self, cleaned_data: pd.DataFrame, summary_data: pd.DataFrame) -> None:
        self.cleaned_data = cleaned_data
        self.summary_data = summary_data

    def detect_anomalies(self, column: str, time_column: str) -> pd.DataFrame:
        self.cleaned_data[time_column] = pd.to_datetime(self.cleaned_data[time_column])

        merged_data = pd.merge_asof(
            self.cleaned_data.sort_values(time_column),
            self.summary_data.sort_values('start_timeframe'),
            by='turbine_id',
            left_on=time_column,
            right_on='start_timeframe',
            direction='backward'
        )

        merged_data['anomaly'] = (
            (merged_data[column] < merged_data['std_dev_start']) |
            (merged_data[column] > merged_data['std_dev_end'])
        )

        # Remove any redundant timestamp column if present
        if 'timestamp_y' in merged_data.columns:
            del merged_data['timestamp_y']

        return merged_data
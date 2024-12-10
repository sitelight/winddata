import pandas as pd
from typing import Any

class AnomalyDetector:
    """
    A utility class to detect anomalies in the dataset using summary statistics.
    Compares each record in the original dataset with the corresponding
    start and end of the standard deviation range from the summarized data.
    """

    def __init__(self, cleaned_data: pd.DataFrame, summary_data: pd.DataFrame) -> None:
        """
        Initialize the AnomalyDetector with cleaned and summary data.
        Args:
            cleaned_data (pd.DataFrame): The original cleaned dataset.
            summary_data (pd.DataFrame): The summarized dataset with std_dev_start and std_dev_end.
        """
        self.cleaned_data = cleaned_data
        self.summary_data = summary_data

    def detect_anomalies(self, column: str, time_column: str) -> pd.DataFrame:
        """
        Detect anomalies based on the provided column and timestamp.
        Args:
            column (str): The column to check for anomalies.
            time_column (str): The timestamp column for merging datasets.
        Returns:
            pd.DataFrame: The merged dataset with anomaly detection results.
        """
        self.cleaned_data[time_column] = pd.to_datetime(self.cleaned_data[time_column])

        merged_data = pd.merge_asof(
            self.cleaned_data.sort_values(time_column),
            self.summary_data.sort_values("start_timeframe"),
            by="turbine_id",
            left_on=time_column,
            right_on="start_timeframe",
            direction="backward"
        )

        merged_data["anomaly"] = (
            (merged_data[column] < merged_data["std_dev_start"]) |
            (merged_data[column] > merged_data["std_dev_end"])
        )

        del merged_data['timestamp_y']
        return merged_data

import pandas as pd
from typing import Any

class DataSummarizer:
    def __init__(self, dataframe: pd.DataFrame) -> None:
        self.dataframe = dataframe

    def group_by_time_window(self, column: str, time_column: str, grouping_window_in_hours: int, std_value: float) -> pd.DataFrame:
        """
        Groups data by a specified time window and calculates statistics.
        Args:
            column (str): The column to calculate statistics for.
            time_column (str): The timestamp column for grouping.
            grouping_window_in_hours (int): The time window size in hours.
            std_value (float): The number of standard deviations for anomaly calculation.
        Returns:
            pd.DataFrame: The summarized dataset with calculated statistics.
        """
        self.dataframe[time_column] = pd.to_datetime(self.dataframe[time_column])
        self.dataframe['recorddat'] = self.dataframe[time_column]

        freq = f"{grouping_window_in_hours}h"
        grouped = self.dataframe.set_index(time_column).groupby(
            ['turbine_id', pd.Grouper(freq=freq)]
        )

        summary = grouped.agg(
            {
                column: ['min', 'max', 'mean', 'std'],
                'recorddat': ['min', 'max'],
            }
        ).reset_index()

        del self.dataframe["recorddat"]

        summary.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col for col in summary.columns]
        summary.rename(columns={
            f"{column}_min": f"{column}_min",
            f"{column}_max": f"{column}_max",
            f"{column}_mean": f"{column}_average",
            f"recorddat_min": "start_timeframe",
            f"recorddat_max": "end_timeframe",
        }, inplace=True)

        summary["freq"] = freq
        summary["std_dev"] = std_value
        summary["std_dev_start"] = summary[f"{column}_average"] - (std_value * summary[f"{column}_std"])
        summary["std_dev_end"] = summary[f"{column}_average"] + (std_value * summary[f"{column}_std"])

        return summary

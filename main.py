import os
import pandas as pd
from typing import List
from data_cleaner import DataCleaner
from data_summary import DataSummarizer
from data_anomaly import AnomalyDetector
from datetime import datetime

directory: str = r"D:\Jean Documents\WindData\sourcedata"
agg_window_in_hours: int = 24
std_deviation: float = 2

csv_files: List[str] = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.csv')]
dataframes: List[pd.DataFrame] = [pd.read_csv(file) for file in csv_files]
merged_data: pd.DataFrame = pd.concat(dataframes, ignore_index=True)

cleaner = DataCleaner(merged_data)
columns_to_clean: List[str] = ["turbine_id", "wind_speed", "wind_direction", "power_output"]
cleaner.clean_data(columns_to_clean)
cleaned_data: pd.DataFrame = cleaner.get_cleaned_data()

summarizer = DataSummarizer(cleaned_data)
grouped_summary: pd.DataFrame = summarizer.group_by_time_window(
    column="power_output",
    time_column="timestamp",
    grouping_window_in_hours=agg_window_in_hours,
    std_value=std_deviation
)

output_dir: str = r"D:\Jean Documents\WindData\destination"
os.makedirs(output_dir, exist_ok=True)
filetime: str = datetime.now().strftime("%d%m%Y_%H%M")
summaryfilename: str = f"summary_{filetime}.csv"
output_file: str = os.path.join(output_dir, summaryfilename)
grouped_summary.to_csv(output_file, index=False)
print(f"Summary has been saved to {output_file}")

detector = AnomalyDetector(cleaned_data, grouped_summary)
anomalies_detected: pd.DataFrame = detector.detect_anomalies(
    column="power_output",
    time_column="timestamp"
)

anomalyfilename: str = f"anomaly_{filetime}.csv"
output_file = os.path.join(output_dir, anomalyfilename)
anomalies_detected.to_csv(output_file, index=False)

print(anomalies_detected)
print(f"Anomalies have been saved to {output_file}")

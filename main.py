import os
import pandas as pd
import logging
from typing import List
from datetime import datetime
from data_cleaner import DataCleaner
from data_summary import DataSummarizer
from data_anomaly import AnomalyDetector

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

# Configuration
CONFIG = {
    'source_directory': r'D:\Jean Documents\WindData\sourcedata',
    'destination_directory': r'D:\Jean Documents\WindData\destination',
    'columns_to_clean': ['turbine_id', 'wind_speed', 'wind_direction', 'power_output'],
    'grouping_window_in_hours': 24,
    'std_deviation_multiplier': 2.0,
    'timestamp_column': 'timestamp',
    'target_column': 'power_output'
}


def read_and_merge_csv_files(source_directory: str) -> pd.DataFrame:
    """Read all CSV files in the source directory and merge into a single DataFrame."""
    try:
        csv_files: List[str] = [
            os.path.join(source_directory, f) 
            for f in os.listdir(source_directory) 
            if f.endswith('.csv')
        ]
        if not csv_files:
            logging.warning('No CSV files found in the source directory.')
            return pd.DataFrame()

        dataframes: List[pd.DataFrame] = [pd.read_csv(file) for file in csv_files]
        merged_data: pd.DataFrame = pd.concat(dataframes, ignore_index=True)
        return merged_data
    except Exception as e:
        logging.error(f'Error reading/merging CSV files: {e}')
        return pd.DataFrame()


def store_in_database(data: pd.DataFrame, table_name: str) -> None:
    """
    Placeholder function to store data in a database.
    Implementation depends on the chosen database and its Python connector.
    """
    logging.info(f'Simulated storing {len(data)} records into database table {table_name}.')


def write_csv(data: pd.DataFrame, directory: str, prefix: str) -> str:
    """Write a DataFrame to a timestamped CSV file in the given directory."""
    os.makedirs(directory, exist_ok=True)
    file_timestamp: str = datetime.now().strftime('%d%m%Y_%H%M')
    filename: str = f'{prefix}_{file_timestamp}.csv'
    output_path: str = os.path.join(directory, filename)
    data.to_csv(output_path, index=False)
    logging.info(f'{prefix.capitalize()} has been saved to {output_path}')
    return output_path


def main() -> None:
    # Step 1: Read and merge data
    raw_data: pd.DataFrame = read_and_merge_csv_files(CONFIG['source_directory'])
    if raw_data.empty:
        logging.error('No data available to process.')
        return

    # Step 2: Clean the data
    cleaner = DataCleaner(raw_data)
    cleaner.clean_data(CONFIG['columns_to_clean'])
    cleaned_data: pd.DataFrame = cleaner.get_cleaned_data()

    if cleaned_data.empty:
        logging.error('No cleaned data available after cleaning steps.')
        return

    # Step 3: Summarize the data
    summarizer = DataSummarizer(cleaned_data)
    grouped_summary: pd.DataFrame = summarizer.group_by_time_window(
        column=CONFIG['target_column'],
        time_column=CONFIG['timestamp_column'],
        grouping_window_in_hours=CONFIG['grouping_window_in_hours'],
        std_value=CONFIG['std_deviation_multiplier']
    )

    # Step 4: Detect anomalies
    detector = AnomalyDetector(cleaned_data, grouped_summary)
    anomalies_detected: pd.DataFrame = detector.detect_anomalies(
        column=CONFIG['target_column'],
        time_column=CONFIG['timestamp_column']
    )

    # Step 5: Write output
    write_csv(grouped_summary, CONFIG['destination_directory'], 'summary')
    anomaly_file = write_csv(anomalies_detected, CONFIG['destination_directory'], 'anomaly')

    # Optional: Store in database
    # store_in_database(cleaned_data, 'cleaned_data_table')
    # store_in_database(grouped_summary, 'summary_table')
    # store_in_database(anomalies_detected, 'anomalies_table')


if __name__ == '__main__':
    main()
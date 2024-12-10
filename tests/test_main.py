import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from main import read_and_merge_csv_files, write_csv

def test_read_and_merge_csv_files(tmp_path):
    # Create mock CSV files
    data1 = pd.DataFrame({'turbine_id': [1], 'timestamp': ['2021-01-01 01:00:00'], 'power_output': [100]})
    data2 = pd.DataFrame({'turbine_id': [2], 'timestamp': ['2021-01-01 02:00:00'], 'power_output': [200]})

    file1 = tmp_path / "data1.csv"
    file2 = tmp_path / "data2.csv"
    data1.to_csv(file1, index=False)
    data2.to_csv(file2, index=False)

    merged_df = read_and_merge_csv_files(str(tmp_path))
    assert len(merged_df) == 2
    assert sorted(merged_df['turbine_id'].unique()) == [1, 2]

def test_write_csv(tmp_path):
    df = pd.DataFrame({'col': [1, 2, 3]})
    output_path = write_csv(df, str(tmp_path), 'test')
    assert os.path.exists(output_path)
    df_out = pd.read_csv(output_path)
    assert df_out.equals(df)
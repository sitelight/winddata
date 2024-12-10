import pandas as pd
import pytest
from data_summary import DataSummarizer

def test_group_by_time_window():
    df = pd.DataFrame({
        'turbine_id': [1, 1, 1],
        'timestamp': [
            '2021-01-01 01:00:00', 
            '2021-01-01 12:00:00', 
            '2021-01-02 01:00:00'
        ],
        'power_output': [100, 110, 105]
    })
    summarizer = DataSummarizer(df)
    summary_df = summarizer.group_by_time_window(
        column='power_output',
        time_column='timestamp',
        grouping_window_in_hours=24,
        std_value=2.0
    )

    # Check that grouping occurred and columns exist
    assert 'power_output_min' in summary_df.columns
    assert 'power_output_max' in summary_df.columns
    assert 'power_output_average' in summary_df.columns
    assert 'std_dev_start' in summary_df.columns
    assert 'std_dev_end' in summary_df.columns
    # Should have 2 groups: one for Jan 1 and one for Jan 2
    assert len(summary_df) == 2
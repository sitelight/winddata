import pandas as pd
import pytest
from data_cleaner import DataCleaner

def test_remove_invalid_records():
    df = pd.DataFrame({
        'turbine_id': ['1', '2', 'x', '4'],
        'wind_speed': ['10.5', 'notnum', '5.0', '3.2'],
        'power_output': ['100', '200', 'abc', '50']
    })
    cleaner = DataCleaner(df)
    cleaner.remove_invalid_records(['turbine_id', 'wind_speed', 'power_output'])
    cleaned_df = cleaner.get_cleaned_data()
    # 'x' and 'abc' are not numeric, 'notnum' also not numeric
    # Only rows with fully numeric values should remain: rows 0 and 3
    assert len(cleaned_df) == 2
    assert all(pd.to_numeric(cleaned_df['turbine_id'], errors='coerce').notna())
    assert all(pd.to_numeric(cleaned_df['wind_speed'], errors='coerce').notna())
    assert all(pd.to_numeric(cleaned_df['power_output'], errors='coerce').notna())

def test_remove_duplicates():
    df = pd.DataFrame({
        'turbine_id': [1, 1, 2, 2],
        'wind_speed': [10, 10, 5, 5],
        'power_output': [100, 100, 200, 200]
    })
    cleaner = DataCleaner(df)
    cleaner.remove_duplicates()
    cleaned_df = cleaner.get_cleaned_data()
    # Duplicates should be removed, leaving only unique rows
    assert len(cleaned_df) == 2

def test_drop_missing_rows():
    df = pd.DataFrame({
        'turbine_id': [1, None, 2],
        'wind_speed': [10.0, 5.0, None],
        'power_output': [100, 50, 200]
    })
    cleaner = DataCleaner(df)
    cleaner.drop_missing_rows(columns=['turbine_id', 'wind_speed'])
    cleaned_df = cleaner.get_cleaned_data()
    # Only rows with no missing in turbine_id and wind_speed remain
    assert len(cleaned_df) == 1
    assert cleaned_df.iloc[0]['turbine_id'] == 1
    assert cleaned_df.iloc[0]['wind_speed'] == 10.0
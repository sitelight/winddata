import pandas as pd
from typing import List, Optional


class DataCleaner:
    """
    Cleans a DataFrame by:
    - Removing non-numeric records in specified columns.
    - Removing duplicates.
    - Dropping rows with missing values.
    """

    def __init__(self, dataframe: pd.DataFrame) -> None:
        self.dataframe = dataframe

    def remove_invalid_records(self, columns: List[str]) -> None:
        for col in columns:
            self.dataframe = self.dataframe[
                self.dataframe[col].apply(lambda x: pd.to_numeric(x, errors='coerce')).notna()
            ]

    def remove_duplicates(self) -> None:
        self.dataframe.drop_duplicates(inplace=True)

    def drop_missing_rows(self, columns: Optional[List[str]] = None) -> None:
        self.dataframe.dropna(subset=columns, inplace=True)

    def clean_data(self, columns_to_check: List[str]) -> None:
        self.remove_invalid_records(columns_to_check)
        self.remove_duplicates()
        self.drop_missing_rows(columns=columns_to_check)

    def get_cleaned_data(self) -> pd.DataFrame:
        return self.dataframe
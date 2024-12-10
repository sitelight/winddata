import pandas as pd
from typing import List, Optional

class DataCleaner:
    """
    This class will clean data with specified logic:
    1. Removing duplicates
    2. Removing records with invalid values (non-numeric values)
    """

    def __init__(self, dataframe: pd.DataFrame) -> None:
        self.dataframe = dataframe

    def remove_invalid_records(self, columns: List[str]) -> None:
        for column in columns:
            self.dataframe = self.dataframe[
                self.dataframe[column].apply(lambda x: pd.to_numeric(x, errors="coerce")).notna()
            ]

    def remove_duplicates(self) -> None:
        self.dataframe.drop_duplicates(inplace=True)

    def drop_missing_rows(self, columns: Optional[List[str]] = None) -> None:
        self.dataframe.dropna(subset=columns, inplace=True)

    def clean_data(self, columns_to_check: List[str]) -> None:
        print("Step 1: Removing invalid records...")
        self.remove_invalid_records(columns_to_check)

        print("Step 2: Removing duplicate records...")
        self.remove_duplicates()

        print("Step 3: Dropping rows with missing values...")
        self.drop_missing_rows(columns=columns_to_check)

    def get_cleaned_data(self) -> pd.DataFrame:
        return self.dataframe

import pandas as pd
import numpy as np
class ConsistencyChecker:
    def __init__(self, data: pd.DataFrame) -> None:
        self.df = data

    def check_duplicate_rows(self) -> bool:
        return self.df.duplicated().any()

    def check_column_datatypes(self, expected_datatypes: dict) -> bool:
        actual_datatypes = self.df.dtypes.to_dict()
        return actual_datatypes == expected_datatypes

    def check_missing_values(self) -> bool:
        return self.df.isnull().any().any()
    
    def drop_duplicates(self) ->pd.DataFrame:
        if self.check_duplicate_rows():
            self.df = self.df.drop_duplicates()
        return self.df

    def check_column_values(self, column_name: str, allowed_values: list) -> bool:
        return all(value in allowed_values for value in self.df[column_name])
    
    def treat_outliers(self, column_name: str, method: str = 'median', z_threshold: float = 3.0) -> pd.DataFrame:
        if method not in ['mean', 'median']:
            raise ValueError("Invalid method. Choose 'mean' or 'median'.")

        column_values = self.df[column_name]
        z_scores = np.abs((column_values - column_values.mean()) / column_values.std())
        outliers_mask = z_scores > z_threshold

        if method == 'mean':
            replacement_value = column_values.mean()
        else:
            replacement_value = column_values.median()

        self.df.loc[outliers_mask, column_name] = replacement_value
        return self.df

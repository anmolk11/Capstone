import pandas as pd
import numpy as np

class MetaData:
    def __init__(self, data: pd.DataFrame):
        self.df = data

    def get_basic_info(self):
        return self.df.info()

    def get_summary_statistics(self):
        return self.df.describe()

    def get_column_types(self):
        return self.df.dtypes

    def get_null_counts(self):
        return self.df.isnull().sum()

    def get_unique_values(self):
        unique_values = {}
        for col in self.df.columns:
            unique_values[col] = self.df[col].unique()
        return unique_values

    def get_column_names(self):
        return self.df.columns.tolist()

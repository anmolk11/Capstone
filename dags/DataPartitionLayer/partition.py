import pandas as pd
import numpy as np


class Partition:
    def __init__(self,data : pd.DataFrame):
        self.df = data 
    
    def partitionByDate(self,date_col: str, parts: int) -> list:
        df = self.df
        df = df.sort_values(by=date_col)
        rows_per_partition = len(df) // parts
        partitions = []

        for i in range(parts):
            start_index = i * rows_per_partition
            end_index = (i + 1) * rows_per_partition if i < parts - 1 else len(df)
            partition = df.iloc[start_index:end_index]
            partitions.append(partition)

        return partitions

    def partitionByCategory(self,category_col: str) -> list:
        df[category_col] = df[category_col].astype('category')
        unique_categories = df[category_col].cat.categories
        partitions = []

        for category in unique_categories:
            category_partition = df[df[category_col] == category]
            partitions.append(category_partition)

        return partitions


if __name__ == '__main__':
    pass

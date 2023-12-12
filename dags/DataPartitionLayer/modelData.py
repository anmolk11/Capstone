import pandas as pd


class Model:
    def __init__(self,data : pd.DataFrame):
        self.df = data
    
    from sklearn.model_selection import train_test_split

    def split_dataframe(df, train_ratio=0.7, test_ratio=0.15, validation_ratio=0.15, random_state=None):
        assert train_ratio + test_ratio + validation_ratio == 1, "Ratios must sum to 1."
        
        train_set, temp_set = train_test_split(df, test_size=(1 - train_ratio), random_state=random_state)
        test_validation_size = test_ratio + validation_ratio
        temp_size = len(temp_set)
        test_size = int((test_ratio / test_validation_size) * temp_size)
        validation_size = temp_size - test_size

        test_set, validation_set = train_test_split(temp_set, test_size=test_size, random_state=random_state)

        return train_set, test_set, validation_set



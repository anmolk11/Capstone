import pandas as pd 
import numpy as np
from sklearn.preprocessing import StandardScaler,LabelEncoder, OneHotEncoder
from sklearn.impute import SimpleImputer


class Transformation:
    def __init__(self, data: pd.DataFrame) -> None:
        self.df = data
    
    def fill_missing_values(self) -> pd.DataFrame:
        imputer = SimpleImputer(strategy='most_frequent')  # For categorical features
        cat_cols = self.df.select_dtypes(include='object').columns
        self.df[cat_cols] = imputer.fit_transform(self.df[cat_cols])

        imputer = SimpleImputer(strategy='median')  # For numeric features with outliers
        num_cols_with_outliers = [col for col in self.df.columns if not self.has_outliers(col)]
        self.df[num_cols_with_outliers] = imputer.fit_transform(self.df[num_cols_with_outliers])

        imputer = SimpleImputer(strategy='mean')  # For numeric features without outliers
        num_cols_without_outliers = [col for col in self.df.columns if self.has_outliers(col)]
        self.df[num_cols_without_outliers] = imputer.fit_transform(self.df[num_cols_without_outliers])

        return self.df

    def has_outliers(self, column_name: str, z_threshold: float = 3.0) -> bool:
        z_scores = np.abs((self.df[column_name] - self.df[column_name].mean()) / self.df[column_name].std())
        return any(z_scores > z_threshold)

    def drop_features(self, to_drop: list) -> pd.DataFrame:
        return self.df.drop(columns=to_drop, inplace=False)

    def drop_features_with_threshold(self, threshold: float) -> pd.DataFrame:
        missing_percentage = (self.df.isnull().sum() / len(self.df)) * 100
        columns_to_drop = missing_percentage[missing_percentage > threshold].index.tolist()
        return self.df.drop(columns=columns_to_drop, inplace=False)
    
    def encode_features(self, unique_values_threshold: int = 15) -> pd.DataFrame:
        encoded_df = pd.DataFrame()
        
        for col in self.df.select_dtypes(include=['object']).columns:
            unique_values_count = self.df[col].nunique()
            
            if unique_values_count > unique_values_threshold:
                label_encoder = LabelEncoder()
                encoded_df[col] = label_encoder.fit_transform(self.df[col])
            else:
                one_hot_encoder = OneHotEncoder(drop='first', sparse=False)
                encoded_values = one_hot_encoder.fit_transform(self.df[[col]])
                encoded_df = pd.concat([encoded_df, pd.DataFrame(encoded_values, columns=one_hot_encoder.get_feature_names_out([col]))], axis=1)

        result_df = pd.concat([self.df, encoded_df], axis=1)
    
        result_df = result_df.drop(columns=self.df.select_dtypes(include=['object']).columns)

        return result_df
    
    def scale_features(self) -> pd.DataFrame:
        scaler = StandardScaler()
        return pd.DataFrame(scaler.fit_transform(self.df), columns=self.df.columns)


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
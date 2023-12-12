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
        if len(cat_cols) > 0:
            self.df[cat_cols] = imputer.fit_transform(self.df[cat_cols])

        imputer = SimpleImputer(strategy='median')  # For numeric features with outliers
        num_cols = self.df.select_dtypes(exclude='object').columns
        if len(num_cols) > 0:
            self.df[num_cols] = imputer.fit_transform(self.df[num_cols])

        # imputer = SimpleImputer(strategy='mean')  # For numeric features without outliers
        # num_cols_without_outliers = [col for col in self.df.columns if self.has_outliers(col)]
        # self.df[num_cols_without_outliers] = imputer.fit_transform(self.df[num_cols_without_outliers])

        return self.df

    def has_outliers(self, column_name: str, z_threshold: float = 3.0) -> bool:
        z_scores = np.abs((self.df[column_name] - self.df[column_name].mean()) / self.df[column_name].std())
        return any(z_scores > z_threshold)

    def drop_features(self, to_drop: list) -> pd.DataFrame:
        self.df = self.df.drop(columns=to_drop, axis = 1)
        return self.df

    def drop_features_with_threshold(self, threshold: float) -> pd.DataFrame:
        missing_percentage = (self.df.isnull().sum() / len(self.df)) * 100
        columns_to_drop = missing_percentage[missing_percentage > threshold].index.tolist()
        self.df = self.df.drop(columns=columns_to_drop, axis = 1)
        return self.df
    
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

        self.df = result_df
        return self.df
    
    def scale_features(self) -> pd.DataFrame:
        scaler = StandardScaler()
        self.df = pd.DataFrame(scaler.fit_transform(self.df), columns=self.df.columns)
        return self.df

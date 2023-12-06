import pandas as pd 
import numpy as np


class Transformation:
    def __init__(self,data : pd.DataFrame):
        self.df = data
    
    def fill_missing_values(self) -> pd.DataFrame:
        pass

    def drop_features(self,to_drop : list) -> pd.DataFrame:
        pass 
    
        
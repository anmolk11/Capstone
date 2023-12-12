import pandas as pd 

def export(df : pd.DataFrame,folder_name : str,file_name : str) -> None:
    path = f'../Warehouse/{folder_name}/{file_name}.csv'
    df.to_csv(path)
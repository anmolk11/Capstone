import pandas as pd 
import os

def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

def exportCSV(df: pd.DataFrame, folder_name: str, file_name: str) -> None:
    folder_path = f'Warehouse/{folder_name}'
    
    create_folder_if_not_exists(folder_path)

    if file_name.endswith('.csv'):
        path = f'{folder_path}/{file_name}'
    else:
        path = f'{folder_path}/{file_name}.csv'

    path = os.path.join(os.getcwd(), path)
    df.to_csv(path)

def exportMetaData(data, folder_name: str, file_name: str) -> None:
    if data == None:
        return
    
    warehouse_path = 'Warehouse/MetaData'
    folder_path = f'{warehouse_path}/{folder_name}'

    create_folder_if_not_exists(folder_path)
    base_name = os.path.splitext(file_name)[0]   
    path = f'{folder_path}/{base_name}.txt'
    path = os.path.join(os.getcwd(), path)

    with open(path, 'w') as F:
        F.write(data)


def debug(*args):
    print('==============================')
    print(args)
    print('==============================')
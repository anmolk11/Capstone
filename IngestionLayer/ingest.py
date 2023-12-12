import pandas as pd 
import psycopg2
from psycopg2 import sql
import re
from datetime import datetime
import os

db_params = {
    'host': 'localhost',
    'database': 'capstone',
    'user': 'postgres',
    'password': 'anmol147',
}

def HDFS(log_string : str):
    components = log_string.split(" ")
    date_part = components[0]
    date = datetime.strptime(date_part, "%y%m%d")

    time_part = components[1]
    time = datetime.strptime(time_part, "%H%M%S").time()

    info_part = components[2]
    log_level = components[3]
    class_and_method = components[4]
    message = " ".join(components[5:])
    return [date,time,info_part,log_level,class_and_method,message]

def Apache(log_entry : str):
    match = re.match(r'\[(\w{3}) (\w{3} \d{2} \d{2}:\d{2}:\d{2} \d{4})\] \[(\w+)\] (.+)', log_entry)
    if match:
        day, date_string, log_type, error_type = match.groups()
        date_object = datetime.strptime(date_string, "%b %d %H:%M:%S %Y")
        date = date_object.strftime("%Y-%m-%d")
        time = date_object.strftime("%H:%M:%S")
        result = [day, date,time, log_type, error_type]
        return result
    else:
        return "Input string doesn't match the expected format."

def getDataFromPostgreSQL(table_name : str) -> pd.DataFrame:
    query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    return df

def getSystemLog(system_name : str) -> pd.DataFrame:
    path = f'../datasets/System Log/{system_name}/{system_name}.log'

    cols_hdfs = ['Date','Time','Pid','Level','Component','Content']
    cols_apace = ['Day','Date','Time','Level','Content']

    data = []

    with open(path,'r') as F:
        for log in F.readlines():
            if system_name == 'HDFS':
                data.append(HDFS(log))
            else:
                data.append(Apache(log))
                
    if system_name == 'HDFS':
        df = pd.DataFrame(data, columns = cols_hdfs)
    else:
        df = pd.DataFrame(data, columns = cols_apace)

    return df           


def getSensorsData(sensorname : str) -> list:
    path = f'../datasets/Sensors-predictive-maintenance/{sensorname}/'
    if not os.path.exists(path):
        raise FileNotFoundError(f"The folder '{path}' does not exist.")

    csv_files = [f for f in os.listdir(path) if f.endswith('.csv')]

    dataframes = []

    for csv_file in csv_files:
        file_path = os.path.join(path, csv_file)
        df = pd.read_csv(file_path)
        dataframes.append(df)

    return dataframes


if __name__ == '__main__':
    all_sensors = os.listdir('../datasets/Sensors-predictive-maintenance/')
    for sensor in all_sensors:
        print(f'\n{sensor}')
        print('--------------------------')
        all_data = getSensorsData(sensor)
        for data in all_data:
            print(data.shape)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

from IngestionLayer.ingest import *
from IngestionLayer.transformation_services import Transformation


default_args = {
    'owner': 'anmol',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingestSensorData(ti):
    path = os.path.join(os.getcwd(), "dags/datasets/Sensors-predictive-maintenance/")
    all_sensors = os.listdir(path)
    all_files = []
    for sensor in all_sensors:
        sensor_data = getSensorsData(sensor)
        for data in sensor_data:
            file_name = data['file_name']
            df = data['data']
            all_files.append(file_name)
            ti.xcom_push(key = file_name,value = df)
    ti.xcom_push(key = 'all_files',value = all_files)

def transformSensorData(ti):
    all_files = ti.xcom_pull(task_ids = 'ingestSensorData',key = 'all_files')
    for file in all_files:
        data = ti.xcom_pull(task_ids = 'ingestSensorData',key = file)
        transformer = Transformation(data)
        transformer.drop_features_with_threshold(threshold = 25)
        transformer.fill_missing_values()
        transformer.encode_features()
        transformer.scale_features()

def storeMetaData(ti):
    pass

def consistencyCheck(ti):
    pass


with DAG(
    dag_id = 'Capstone',
    default_args = default_args,
    description = 'Capstone ETL Pipelines',
    start_date = datetime(2023, 10, 8),
    schedule_interval = '@daily' 
) as dag:
    task1 = PythonOperator(
        task_id = 'ingestSensorData',
        python_callable = ingestSensorData
    )
    task2 = PythonOperator(
        task_id = 'transformSensorData',
        python_callable = transformSensorData
    )

    task1 >> task2

# if __name__ == "__main__":
#     all_sensors = os.listdir('datasets/Sensors-predictive-maintenance/')
#     for sensor in all_sensors:
#         sensor_data = getSensorsData(sensor)
#         for data in sensor_data:
#             print(data['file_name'])
#             print(data['data'].info())
#             print('----------------------------')
    
    


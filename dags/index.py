from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

from IngestionLayer.ingest import *
from IngestionLayer.transformation_services import Transformation
from IngestionLayer.consistency import ConsistencyChecker
from IngestionLayer.metadata import MetaData

from DataPartitionLayer.export import *

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

def exportRawData(ti):
    all_files = ti.xcom_pull(task_ids = 'ingestSensorData',key = 'all_files')
    for file in all_files:
        data = ti.xcom_pull(task_ids = 'consistencyCheck',key = file)
        exportCSV(df = data,folder_name = 'Sensor',file_name = file)

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
    all_files = ti.xcom_pull(task_ids = 'ingestSensorData',key = 'all_files')
    for file in all_files:
        file_name = os.path.basename(file)
        data = ti.xcom_pull(task_ids = 'consistencyCheck',key = file)
        debug(data)
        meta = MetaData(data)
        basic_info = meta.get_basic_info()
        stats = meta.get_summary_statistics()
        missing = meta.get_null_counts()      

        exportMetaData(basic_info,'Sensors',f'{file_name}_info')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
        exportMetaData(stats,'Sensors',f'{file_name}_stats')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
        exportMetaData(missing,'Sensors',f'{file_name}_missing_values')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       

def consistencyCheck(ti):
    all_files = ti.xcom_pull(task_ids = 'ingestSensorData',key = 'all_files')
    for file in all_files:
        data = ti.xcom_pull(task_ids = 'ingestSensorData',key = file)
        consistent = ConsistencyChecker(data)
        final = consistent.drop_duplicates()
        # final = consistent.treat_outliers()
        ti.xcom_push(key = file,value = final)

def partitionData(ti):
    

with DAG(
    dag_id = 'Capstone-ETL',
    default_args = default_args,
    description = 'Capstone ETL Pipelines',
    start_date = datetime(2023, 10, 8),
    schedule = '@daily' 
) as dag:
    task1 = PythonOperator(
        task_id = 'ingestSensorData',
        python_callable = ingestSensorData
    )
    task2 = PythonOperator(
        task_id = 'consistencyCheck',
        python_callable = consistencyCheck
    )
    task3 = PythonOperator(
        task_id = 'transformSensorData',
        python_callable = transformSensorData
    )
    task4 = PythonOperator(
        task_id = 'exportRawData',
        python_callable = exportRawData
    )
    task5 = PythonOperator(
        task_id = 'storeMetaData',
        python_callable = storeMetaData
    )

    task1 >> task2 >> [task4,task3,task5]


# if __name__ == '__main__':
#     pass
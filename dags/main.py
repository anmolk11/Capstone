from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from IngestionLayer.ingest import 

default_args = {
    'owner': 'anmol',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# if __name__ == "__main__":
#     dag.cli()

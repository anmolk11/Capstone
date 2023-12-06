import psycopg2
import pandas as pd
import numpy as np
from tqdm import tqdm
from psycopg2.extensions import register_adapter, AsIs
register_adapter(np.int64, AsIs)

db_params = {
    'host': 'localhost',
    'database': 'capstone',
    'user': 'postgres',
    'password': 'anmol147',
}

def insert(data_to_insert):
    insert_query = """
    INSERT INTO hr_core (
        "Employee_Name", "Employee Number", "State", "Zip", "DOB", "Age", "Sex",
        "MaritalDesc", "CitizenDesc", "Hispanic/Latino", "RaceDesc",
        "Date of Hire", "Date of Termination", "Reason For Term",
        "Employment Status", "Department", "Position", "Pay Rate",
        "Manager Name", "Employee Source", "Performance Score"
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s
    );
    """
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute(insert_query, data_to_insert)
        connection.commit()
        # print("Data inserted successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

def fix_na(data):
   cleaned_tuple = tuple(None if pd.isna(value) else value for value in data)
   return cleaned_tuple


if __name__ == '__main__':
    df = pd.read_csv('../datasets/HR-Data/core_dataset.csv')
    for index, data in tqdm(df.iterrows(), total=len(df), desc="Inserting", unit="row"):
        data = tuple(data)
        insert(fix_na(data))
        
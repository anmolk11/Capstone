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

def fix_na(data):
   cleaned_tuple = tuple(None if pd.isna(value) else value for value in data)
   return cleaned_tuple

def insert(query,data):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute(query, data)
        connection.commit()
        # print("Data inserted successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

def insert_hr(data_to_insert):
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
    insert(insert_query,data_to_insert)

def insert_energy(data_to_insert):
    insert_query = """
    INSERT INTO energy_consumption (
        date, Usage_kWh, Lagging_Current_Reactive_Power_kVarh, 
        Leading_Current_Reactive_Power_kVarh, CO2_tCO2, 
        Lagging_Current_Power_Factor, Leading_Current_Power_Factor, 
        NSM, WeekStatus, Day_of_week, Load_Type,time
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
    """
    insert(insert_query,data_to_insert)

def insert_supply_chain(data_to_insert):
    insert_query = """
    INSERT INTO supply_chain (
    "Product type", SKU, Price, Availability, "Number of products sold",
    "Revenue generated", "Customer demographics", "Stock levels", "Lead times",
    "Order quantities", "Shipping times", "Shipping carriers", "Shipping costs",
    "Supplier name", Location, "Lead time", "Production volumes",
    "Manufacturing lead time", "Manufacturing costs", "Inspection results","Defect rates","Transportation modes",
    "Routes","Costs"
    ) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
    """
    insert(insert_query,data_to_insert)


def insert_df(path,func):
    df = pd.read_csv(path)
    # df['date'] = pd.to_datetime(df['date'], format = '%d/%m/%Y')
    for index, data in tqdm(df.iterrows(), total=len(df), desc="Inserting", unit="row"):
        data = tuple(data)
        func(fix_na(data))

if __name__ == '__main__':
    # hr_path = '../datasets/HR-Data/core_dataset.csv'
    # energy_path = '../datasets/Energy Consumption/energy.csv'
    supply_chain_path = '../datasets/Supply Chain/supply_chain.csv'
    insert_df(supply_chain_path,insert_supply_chain)
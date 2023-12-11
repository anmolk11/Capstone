import pandas as pd 
import psycopg2
from psycopg2 import sql

db_params = {
    'host': 'localhost',
    'database': 'capstone',
    'user': 'postgres',
    'password': 'anmol147',
}

def getDataFromPostgreSQL(table_name) -> pd.DataFrame:
    query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    return df

def getSystemLog(system_name):
    pass



if __name__ == '__main__':
    df = getDataFromPostgreSQL('hr_core')
    print(df.info())


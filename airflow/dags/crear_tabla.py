from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
import os

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 28),
    'email': ['user@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG('Crear_tabla_2', schedule_interval="@once", default_args=default_args, catchup=False)

def get_data(**kwargs):
    url = "http://10.43.101.149/data?group_number=8"
    resp = requests.get(url)
    if resp.status_code == 200:
        res = resp.json()
        # Guardar los datos en un archivo CSV temporal para inspección
        df = pd.DataFrame(res['data'])
        temp_file_path = '/opt/airflow/data/temp_api_data.csv'
        df.to_csv(temp_file_path, index=False)
        kwargs['ti'].xcom_push(key='temp_file_path', value=temp_file_path)
        return temp_file_path
    return None

def create_table():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    create_sql = """
    CREATE TABLE IF NOT EXISTS test_table_2 (
        brokered_by FLOAT,
        status VARCHAR(255),
        price FLOAT,
        bed FLOAT,
        bath FLOAT,
        acre_lot FLOAT,
        street FLOAT,
        city VARCHAR(255),
        state VARCHAR(255),
        zip_code FLOAT,
        house_size FLOAT,
        prev_sold_date VARCHAR(255),
        inserted_at TIMESTAMP
    );
    """
    postgres_hook.run(create_sql)

def store_data(ti, **kwargs):
    file_path = ti.xcom_pull(task_ids='get_data', key='temp_file_path')
    if not file_path:
        return

    # Leer los datos del archivo CSV
    df = pd.read_csv(file_path)
    current_timestamp = datetime.now()

    # Convertir el timestamp a una cadena de texto con el formato adecuado
    df['inserted_at'] = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')

    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    # Convertir el DataFrame en una lista de tuplas
    data_list = df.to_records(index=False).tolist()

    insert_sql = """
    INSERT INTO test_table_2 (
        brokered_by, status, price, bed, bath, acre_lot,
        street, city, state, zip_code, house_size, prev_sold_date, inserted_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_sql, data_list)
    connection.commit()
    cursor.close()
    connection.close()

    df.to_csv('/opt/airflow/data/resultado.csv', index=False)

def delete_temp_file():
    temp_file_path = '/opt/airflow/data/temp_api_data.csv'
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

# Definición de las tareas
fetch_data_task = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    provide_context=True,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    provide_context=True,
    dag=dag,
)

store_data_task = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    provide_context=True,
    dag=dag,
)

delete_temp_file_task = PythonOperator(
    task_id='delete_temp_file',
    python_callable=delete_temp_file,
    provide_context=True,
    dag=dag,
)

# Definición de la secuencia de tareas
fetch_data_task >> create_table_task >> store_data_task >> delete_temp_file_task

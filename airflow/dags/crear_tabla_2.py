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

def check_and_drop_table():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    check_sql = """
    SELECT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_name = 'test_table'
    );
    """
    cursor.execute(check_sql)
    exists = cursor.fetchone()[0]
    
    if exists:
        drop_sql = "DROP TABLE IF EXISTS test_table;"
        cursor.execute(drop_sql)
        conn.commit()
    
    cursor.close()
    conn.close()

def get_data(**kwargs):
    url = "http://10.43.101.149/data?group_number=8"
    resp = requests.get(url)
    if resp.status_code == 200:
        res = resp.json()
        # Guardar los datos en un archivo CSV temporal para inspección
        df = pd.DataFrame(res['data'])
        df.to_csv('/opt/airflow/data/temp_api_data.csv', index=False)
        return '/opt/airflow/data/temp_api_data.csv'
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
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    postgres_hook.run(create_sql)

def store_data(ti, **kwargs):
    file_path = ti.xcom_pull(task_ids='get_data')
    if not file_path:
        return

    # Leer los datos del archivo CSV
    df = pd.read_csv(file_path)

    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    # Convertir el DataFrame en una lista de tuplas
    data_list = df.to_records(index=False).tolist()

    insert_sql = """
    INSERT INTO test_table_2 (
        brokered_by, status, price, bed, bath, acre_lot,
        street, city, state, zip_code, house_size, prev_sold_date, inserted_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    """
    cursor.executemany(insert_sql, data_list)
    connection.commit()
    cursor.close()
    connection.close()

    df.to_csv('/opt/airflow/data/cross_tableA.csv', index=False)

def delete_temp_file():
    temp_file_path = '/opt/airflow/data/temp_api_data.csv'
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

def save_db(**kwargs):
    query = 'SELECT * FROM test_table_2'
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    df = postgres_hook.get_pandas_df(sql=query)
    print(df)

# Definición de las tareas
check_and_drop_task = PythonOperator(
    task_id='check_and_drop_table',
    python_callable=check_and_drop_table,
    provide_context=True,
    dag=dag,
)

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

print_data_task = PythonOperator(
    task_id='save_db',
    python_callable=save_db,
    provide_context=True,
    dag=dag,
)

bash_task = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello World from Task 1"',
    dag=dag
)

# Definición de la secuencia de tareas
bash_task >> check_and_drop_task >> fetch_data_task >> create_table_task >> store_data_task >> delete_temp_file_task >> print_data_task

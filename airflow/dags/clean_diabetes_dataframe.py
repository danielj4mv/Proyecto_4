from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 18),
    'email': ['user@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG('clean_and_save_data_optimized_2', schedule_interval="@once", default_args=default_args, catchup=False)

def fetch_data_from_db(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    df_iter = pd.read_sql("SELECT * FROM test_table;", postgres_hook.get_conn(), chunksize=10000)
    df_list = [chunk for chunk in df_iter]
    df = pd.concat(df_list, ignore_index=True)
    return df.to_dict(orient='records')

def clean_and_preprocess_data(ti, **kwargs):
    records = ti.xcom_pull(task_ids='fetch_data_from_db')
    df = pd.DataFrame(records)
    
    df.dropna(inplace=True)
    df = df.astype({'brokered_by': float, 'price': float, 'bed': float, 'bath': float, 'acre_lot': float,
                    'street': float, 'zip_code': float, 'house_size': float})

    df['prev_sold_date'] = pd.to_datetime(df['prev_sold_date'], errors='coerce')  # Use coercion to handle different formats
    df['years_since_sold'] = datetime.now().year - df['prev_sold_date'].dt.year
    df['prev_sold_date'] = df['prev_sold_date'].dt.strftime('%Y-%m-%d')

    return df.to_dict(orient='records')

def save_clean_data_to_db(ti, **kwargs):
    cleaned_data = ti.xcom_pull(task_ids='clean_and_preprocess_data')
    if cleaned_data:
        df = pd.DataFrame(cleaned_data)
        df.to_csv('/opt/airflow/data/clean_data.csv', index=False)
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Verificar si la tabla clean_data existe y eliminarla si es así
        drop_table_sql = """
        DROP TABLE IF EXISTS clean_data;
        """
        cursor.execute(drop_table_sql)
        conn.commit()

        # Crear la tabla clean_data
        create_sql = """
        CREATE TABLE clean_data (
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
            prev_sold_date DATE,
            years_since_sold INT
        );
        """
        cursor.execute(create_sql)
        conn.commit()

        # Insertar datos en la tabla clean_data
        insert_sql = '''
            INSERT INTO clean_data (
                brokered_by, status, price, bed, bath, acre_lot,
                street, city, state, zip_code, house_size, prev_sold_date, years_since_sold
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        '''
        batch_size = 1000
        for i in range(0, len(cleaned_data), batch_size):
            batch = cleaned_data[i:i + batch_size]
            parameters = [
                (
                    record['brokered_by'], record['status'], record['price'], record['bed'], 
                    record['bath'], record['acre_lot'], record['street'], record['city'], 
                    record['state'], record['zip_code'], record['house_size'], record['prev_sold_date'],
                    record['years_since_sold']
                ) for record in batch
            ]
            cursor.executemany(insert_sql, parameters)
            conn.commit()

        cursor.close()
        conn.close()

fetch_data_task = PythonOperator(
    task_id='fetch_data_from_db',
    python_callable=fetch_data_from_db,
    provide_context=True,
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='clean_and_preprocess_data',
    python_callable=clean_and_preprocess_data,
    provide_context=True,
    dag=dag,
)

save_clean_data_task = PythonOperator(
    task_id='save_clean_data_to_db',
    python_callable=save_clean_data_to_db,
    provide_context=True,
    dag=dag,
)

fetch_data_task >> clean_data_task >> save_clean_data_task

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine

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

def get_sqlalchemy_conn(postgres_hook):
    connection = postgres_hook.get_connection(postgres_hook.postgres_conn_id)
    return create_engine(f'postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}')

def fetch_data_from_db(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    engine = get_sqlalchemy_conn(postgres_hook)
    
    check_table_query = """
    SELECT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_name = 'clean_data_2'
    );
    """
    
    exists = pd.read_sql(check_table_query, engine).iloc[0, 0]
    
    if not exists:
        query = "SELECT * FROM test_table_2;"
    else:
        last_execution_date_query = "SELECT max(inserted_at) FROM clean_data_2;"
        last_execution_date = pd.read_sql(last_execution_date_query, engine).iloc[0, 0]
        if last_execution_date is None:
            query = "SELECT * FROM test_table_2;"
        else:
            query = f"SELECT * FROM test_table_2 WHERE inserted_at > '{last_execution_date}';"

    print(f"Running query: {query}")  # Registro adicional para ver la consulta ejecutada
    df_iter = pd.read_sql(query, engine, chunksize=10000)
    df_list = [chunk for chunk in df_iter]
    df = pd.concat(df_list, ignore_index=True)

    print(f"Fetched data: {df.head()}")  # Registro adicional para ver los primeros registros obtenidos

    # Convertir los timestamps a cadenas de texto antes de devolver los datos
    if not df.empty:
        df['inserted_at'] = df['inserted_at'].astype(str)
        df['prev_sold_date'] = df['prev_sold_date'].astype(str)
    
    return df.to_dict(orient='records')

def clean_and_preprocess_data(ti, **kwargs):
    records = ti.xcom_pull(task_ids='fetch_data_from_db')
    df = pd.DataFrame(records)

    print(f"Data before cleaning: {df.head()}")  # Registro adicional para ver los datos antes de limpiar

    required_columns = ['brokered_by', 'price', 'bed', 'bath', 'acre_lot', 'street', 'zip_code', 'house_size']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        print(f"Columns in the fetched data: {df.columns.tolist()}")
        raise KeyError(f"Missing columns in the data: {missing_columns}")

    df.dropna(inplace=True)
    df = df.astype({'brokered_by': float, 'price': float, 'bed': float, 'bath': float, 'acre_lot': float,
                    'street': float, 'zip_code': float, 'house_size': float})

    df['prev_sold_date'] = pd.to_datetime(df['prev_sold_date'], errors='coerce')  # Use coercion to handle different formats
    df['years_since_sold'] = datetime.now().year - df['prev_sold_date'].dt.year
    df['prev_sold_date'] = df['prev_sold_date'].dt.strftime('%Y-%m-%d')

    # Convertir el timestamp a una cadena de texto con el formato adecuado
    current_timestamp = datetime.now()
    df['inserted_at'] = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')

    return df.to_dict(orient='records')

def save_clean_data_to_db(ti, **kwargs):
    cleaned_data = ti.xcom_pull(task_ids='clean_and_preprocess_data')
    if cleaned_data:
        df = pd.DataFrame(cleaned_data)
        df.to_csv('/opt/airflow/data/clean_data.csv', index=False)
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
        engine = get_sqlalchemy_conn(postgres_hook)
        
        with engine.connect() as conn:
            # Crear la tabla clean_data_2 si no existe
            create_sql = """
            CREATE TABLE IF NOT EXISTS clean_data_2 (
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
                years_since_sold INT,
                inserted_at TIMESTAMP
            );
            """
            conn.execute(create_sql)

            # Insertar datos en la tabla clean_data_2
            df.to_sql('clean_data_2', engine, if_exists='append', index=False)

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

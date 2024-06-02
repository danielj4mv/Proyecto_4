from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
import os
import tensorflow_data_validation as tfdv

from sklearn.linear_model import Ridge
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Lasso
from sklearn.linear_model import ElasticNet
from sklearn.linear_model import HuberRegressor

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
import mlflow


# Configuración del entorno
os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://s3:9000'
os.environ['AWS_ACCESS_KEY_ID'] = 'mlflows3'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'mlflows3'
os.environ['MLFLOW_TRACKING_URI'] = 'http://mlflow-webserver:5000'

mlflow.set_tracking_uri(os.environ['MLFLOW_TRACKING_URI'])

experiment_name = "house"

# Comprobación y creación de experimento
client = mlflow.tracking.MlflowClient()
experiment = client.get_experiment_by_name(experiment_name)

if experiment is None:
    client.create_experiment(experiment_name)
else:
    if experiment.lifecycle_stage == 'deleted':
        client.restore_experiment(experiment.experiment_id)

mlflow.set_experiment(experiment_name)



def get_sqlalchemy_conn(postgres_hook):
    connection = postgres_hook.get_connection(postgres_hook.postgres_conn_id)
    return create_engine(f'postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}')


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


def check_data(**kwargs):
    temp_file_path = '/opt/airflow/data/temp_api_data.csv'
    return os.path.exists(temp_file_path)


def create_table(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    create_sql = """
    CREATE TABLE IF NOT EXISTS test_table_3 (
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
        prev_sold_date VARCHAR(255)
    );
    """
    postgres_hook.run(create_sql)


def store_data(ti, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    engine = get_sqlalchemy_conn(postgres_hook)

    temp_file_path = '/opt/airflow/data/temp_api_data.csv'
    df = pd.read_csv(temp_file_path)

    df.to_sql('test_table_3', engine, if_exists='append', index=False)



def clean_data(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    engine = get_sqlalchemy_conn(postgres_hook)
    
    create_sql = """
    CREATE TABLE IF NOT EXISTS clean_data_3 (
        brokered_by FLOAT,
        status VARCHAR(255),
        price FLOAT,
        bed FLOAT,
        bath FLOAT,
        acre_lot FLOAT,
        street FLOAT,
        state VARCHAR(255),
        zip_code FLOAT,
        house_size FLOAT
    );
    """
    postgres_hook.run(create_sql)


    temp_file_path = '/opt/airflow/data/temp_api_data.csv'
    df = pd.read_csv(temp_file_path)


    df.dropna(inplace=True)
    df = df.astype({'brokered_by': float, 'price': float, 'bed': float, 'bath': float, 'acre_lot': float,
                    'street': float, 'zip_code': float, 'house_size': float})
    df.drop(columns=['city', 'prev_sold_date'],inplace=True)


    df.to_sql('clean_data_3', engine, if_exists='append', index=False)
    df.to_csv('/opt/airflow/data/clean_data.csv', index=False)


def entrenar(datos, modelo, model_name, razon):
    X = datos.drop('price', axis=1)
    y = datos['price']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    categorical= ['status', 'state']
    numeric = X_train.columns.difference(categorical)

    model=Pipeline(steps=[
        ('preprocessor', ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical),
                ('num', StandardScaler(), numeric)])),
        ('regressor', modelo)
    ])


    model.fit(X_train, y_train)
    r2=model.score(X_test, y_test)
    preprocessor = model.named_steps['preprocessor']
    regressor = model.named_steps['regressor']
    tr = preprocessor.transform(X_test[:10])
    tr_df=pd.DataFrame(tr, columns=list(preprocessor.named_transformers_['cat'].get_feature_names_out())+list(preprocessor.named_transformers_['num'].get_feature_names_out())).head(10)

    with mlflow.start_run():
        mlflow.log_param("model_type", model_name)
        mlflow.log_metric("r2", r2)
        mlflow.sklearn.log_model(model, "model", registered_model_name=model_name)
        mlflow.log_text(razon, "motivo_entrenamiento.txt")
        mlflow.shap.log_explanation(regressor.predict, tr_df)
        run_id=mlflow.active_run().info.run_id
    print('-'*500)
    print({'r2':r2, 'model_name':model_name, 'run_id': run_id})
    print('-'*500)
    return {'r2':r2, 'model_name':model_name, 'run_id': run_id}



def entrenamiento(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    engine = get_sqlalchemy_conn(postgres_hook)
    df_clean=pd.read_sql('clean_data_3', engine)
    print('*'*100)
    print(df_clean.columns)
    print('*'*100)

    temp_file_path = '/opt/airflow/data/clean_data.csv'
    df=pd.read_csv(temp_file_path)

    if df.shape[0] == df_clean.shape[0]:
        razon='se decidió entrenar porque es el primer lote del conjunto de datos'
        lr=entrenar(df_clean, LinearRegression(), 'LinearRegression', razon)
        ridge=entrenar(df_clean, Ridge(), 'Ridge', razon)
        lasso=entrenar(df_clean, Lasso(), 'Lasso', razon)
        elasticnet=entrenar(df_clean, ElasticNet(), 'ElasticNet', razon)
        huber=entrenar(df_clean, HuberRegressor(), 'HuberRegressor', razon)

        metrics = [lr, ridge, lasso, elasticnet, huber]
        best_model = max(metrics, key=lambda x: x['r2'])

        client = mlflow.tracking.MlflowClient()
        model_uri = f"runs:/{best_model['run_id']}/model"
        version_info = client.create_model_version(name=best_model['model_name'], source=model_uri, run_id=best_model['run_id'])
        version = version_info.version
        client.transition_model_version_stage(
            name=best_model['model_name'],
            version=version,
            stage="Production",
            archive_existing_versions=True
        )


    else:
        previous_df_clean=pd.concat([df_clean, df]).drop_duplicates(keep=False)
        clean_stats = tfdv.generate_statistics_from_dataframe(previous_df_clean)
        df_stats = tfdv.generate_statistics_from_dataframe(df)

        schema = tfdv.infer_schema(statistics=clean_stats)
        anomalies = tfdv.validate_statistics(statistics=df_stats, schema=schema)

        has_anomalies = len(anomalies.anomaly_info) > 0
        if has_anomalies:
            #print(anomalies.anomaly_info)
            razon='se decidió entrenar porque se detectaron las siguientes anomalías:\n'+str(anomalies.anomaly_info)
            lr=entrenar(df_clean, LinearRegression(), 'LinearRegression', razon)
            ridge=entrenar(df_clean, Ridge(), 'Ridge', razon)
            lasso=entrenar(df_clean, Lasso(), 'Lasso', razon)
            elasticnet=entrenar(df_clean, ElasticNet(), 'ElasticNet', razon)
            huber=entrenar(df_clean, HuberRegressor(), 'HuberRegressor', razon)
            metrics = [lr, ridge, lasso, elasticnet, huber]
            best_model = max(metrics, key=lambda x: x['r2'])

            client = mlflow.tracking.MlflowClient()
            model_uri = f"runs:/{best_model['run_id']}/model"
            version_info = client.create_model_version(name=best_model['model_name'], source=model_uri, run_id=best_model['run_id'])
            version = version_info.version
            client.transition_model_version_stage(
                name=best_model['model_name'],
                version=version,
                stage="Production",
                archive_existing_versions=True
            )


def delete_temp_file():
    temp_file_path = '/opt/airflow/data/temp_api_data.csv'
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'repeat_tasks_dag',
    default_args=default_args,
    description='A simple DAG to repeat tasks',
    schedule_interval= '@once',
    start_date=datetime(2023, 6, 1),
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )

    previous_task = start

    for i in range(15):
        fetch_data_task = PythonOperator(
            task_id=f'get_data_{i}',
            python_callable=get_data,
            provide_context=True,
            op_kwargs={'loop_index': i},
        )

        check_task = ShortCircuitOperator(
            task_id=f'check_data_{i}',
            python_callable=check_data,
            provide_context=True,
            op_kwargs={'loop_index': i},
        )

        create_table_task = PythonOperator(
            task_id=f'create_table_{i}',
            python_callable=create_table,
            provide_context=True,
            op_kwargs={'loop_index': i},
        )

        store_data_task = PythonOperator(
            task_id=f'store_data_{i}',
            python_callable=store_data,
            provide_context=True,
            op_kwargs={'loop_index': i},
        )

        clean_data_task = PythonOperator(
            task_id=f'clean_data_{i}',
            python_callable=clean_data,
            provide_context=True,
            op_kwargs={'loop_index': i},
        )

        entrenamiento_task = PythonOperator(
            task_id=f'entrenamiento_{i}',
            python_callable=entrenamiento,
            provide_context=True,
            op_kwargs={'loop_index': i},
        )



        delete_temp_file_task = PythonOperator(
            task_id=f'delete_temp_file_{i}',
            python_callable=delete_temp_file,
            provide_context=True,
            op_kwargs={'loop_index': i},
        )






        # Establecer las dependencias de las tareas en serie para cada ciclo
        previous_task >> fetch_data_task
        fetch_data_task >> check_task
        check_task >> create_table_task
        create_table_task >> store_data_task
        store_data_task >> clean_data_task
        clean_data_task >> entrenamiento_task
        entrenamiento_task >> delete_temp_file_task
        
        previous_task = delete_temp_file_task

    previous_task >> end



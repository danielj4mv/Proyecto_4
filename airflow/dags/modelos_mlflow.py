import os
import mlflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd
import xgboost as xgb
import shap
from datetime import datetime, timedelta
import json

# Configuración del entorno
os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://s3:9000'
os.environ['AWS_ACCESS_KEY_ID'] = 'mlflows3'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'mlflows3'
os.environ['MLFLOW_TRACKING_URI'] = 'http://mlflow-webserver:5000'

mlflow.set_tracking_uri(os.environ['MLFLOW_TRACKING_URI'])

experiment_name = "real_estate_model_training"

# Comprobación y creación de experimento
client = mlflow.tracking.MlflowClient()
experiment = client.get_experiment_by_name(experiment_name)

if experiment is None:
    client.create_experiment(experiment_name)
else:
    if experiment.lifecycle_stage == 'deleted':
        client.restore_experiment(experiment.experiment_id)

mlflow.set_experiment(experiment_name)

default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 3, 28),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'train_real_estate_models_3',
    default_args=default_args,
    description='Train machine learning models with sampling and track with MLflow',
    schedule_interval='@weekly',
    catchup=False
)

def fetch_data():
    hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
    df = hook.get_pandas_df("SELECT * FROM clean_data")

    df['prev_sold_date'] = pd.to_datetime(df['prev_sold_date'], errors='coerce')
    df['prev_sold_date'] = df['prev_sold_date'].apply(lambda x: x.toordinal() if pd.notnull(x) else x)
    
    df = df.select_dtypes(include=['float64', 'int64'])
    
    return df

def sample_data(**kwargs):
    ti = kwargs['ti']
    df = fetch_data()
    
    df_sampled = df.sample(n=10000, random_state=42)
    
    ti.xcom_push(key='sampled_data', value=df_sampled.to_dict(orient='records'))

def train_model(model, model_name, df, **kwargs):
    X = df.drop('price', axis=1)
    y = df['price']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    with mlflow.start_run():
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)
        mlflow.log_param("model_type", model_name)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)
        mlflow.sklearn.log_model(model, "model", registered_model_name=model_name)

        ti = kwargs['ti']
        ti.xcom_push(key=f"{model_name}_metrics", value={
            'r2': r2, 
            'mse': mse, 
            'model_name': model_name, 
            'run_id': mlflow.active_run().info.run_id,
            'X_train': X_train.to_dict(),
            'X_test': X_test.to_dict()
        })

def train_random_forest(**kwargs):
    ti = kwargs['ti']
    sampled_data = ti.xcom_pull(task_ids='sample_data', key='sampled_data')
    df_sampled = pd.DataFrame(sampled_data)
    train_model(RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1), "Random_Forest_Model", df_sampled, **kwargs)

def train_decision_tree(**kwargs):
    ti = kwargs['ti']
    sampled_data = ti.xcom_pull(task_ids='sample_data', key='sampled_data')
    df_sampled = pd.DataFrame(sampled_data)
    train_model(DecisionTreeRegressor(random_state=42), "Decision_Tree_Model", df_sampled, **kwargs)

def train_xgboost(**kwargs):
    ti = kwargs['ti']
    sampled_data = ti.xcom_pull(task_ids='sample_data', key='sampled_data')
    df_sampled = pd.DataFrame(sampled_data)
    train_model(xgb.XGBRegressor(n_estimators=100, random_state=42, n_jobs=-1), "XGBoost_Model", df_sampled, **kwargs)

def evaluate_models(**kwargs):
    ti = kwargs['ti']
    rf_metrics = ti.xcom_pull(task_ids='train_random_forest', key='Random_Forest_Model_metrics')
    dt_metrics = ti.xcom_pull(task_ids='train_decision_tree', key='Decision_Tree_Model_metrics')
    xgb_metrics = ti.xcom_pull(task_ids='train_xgboost', key='XGBoost_Model_metrics')

    metrics = [rf_metrics, dt_metrics, xgb_metrics]
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

    cursor = None
    conn = None
    try:
        X_train = pd.DataFrame(best_model['X_train'])
        X_test = pd.DataFrame(best_model['X_test'])
        model = mlflow.sklearn.load_model(model_uri)
        explainer = shap.Explainer(model, X_train)
        shap_values = explainer(X_test, check_additivity=False)
        shap_summary = shap_values.mean(0).values.tolist()

        hook = PostgresHook(postgres_conn_id='postgres_test_conn', schema='airflow')
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS shap_values (
                model_name TEXT,
                shap_values JSON
            )
        """)
        cursor.execute("""
            INSERT INTO shap_values (model_name, shap_values) VALUES (%s, %s)
        """, (best_model['model_name'], json.dumps(shap_summary)))

        # Guardar los metadatos del modelo en PostgreSQL
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS model_metadata (
                model_name TEXT,
                r2 FLOAT,
                mse FLOAT,
                run_id TEXT
            )
        """)
        cursor.execute("""
            INSERT INTO model_metadata (model_name, r2, mse, run_id) VALUES (%s, %s, %s, %s)
        """, (best_model['model_name'], best_model['r2'], best_model['mse'], best_model['run_id']))

        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

task_fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag
)

task_sample_data = PythonOperator(
    task_id='sample_data',
    python_callable=sample_data,
    provide_context=True,
    dag=dag
)

task_train_rf = PythonOperator(
    task_id='train_random_forest',
    python_callable=train_random_forest,
    provide_context=True,
    dag=dag
)

task_train_dt = PythonOperator(
    task_id='train_decision_tree',
    python_callable=train_decision_tree,
    provide_context=True,
    dag=dag
)

task_train_xgb = PythonOperator(
    task_id='train_xgboost',
    python_callable=train_xgboost,
    provide_context=True,
    dag=dag
)

task_evaluate = PythonOperator(
    task_id='evaluate_models',
    python_callable=evaluate_models,
    provide_context=True,
    dag=dag
)

task_fetch_data >> task_sample_data >> [task_train_rf, task_train_dt, task_train_xgb] >> task_evaluate

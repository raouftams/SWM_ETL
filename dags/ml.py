from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago
import sys, os

def test_function():
    pass

#defining default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,3,13),
    'email': ['raouftams@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(days=30)
}

#creating dag for rotation data extraction from files (where to store files ?)
with DAG(
    'ML_DAG',
    default_args=default_args,
    description='Models training pipeline',
    schedule_interval=timedelta(days=30)
) as dag:

    #creating tasks
    extract = PythonOperator(
        task_id='Extract_new_data',
        python_callable=test_function,
        dag=dag
    )

    anomaly = PythonOperator(
        task_id='Anomaly_detection_cleaning',
        python_callable=test_function,
        dag=dag
    )

    prediction_prepare = PythonOperator(
        task_id='Prepare_prediction_data',
        python_callable=test_function,
        dag=dag
    )

    kmeans_prepare = PythonOperator(
        task_id='Prepare_kmeans_data',
        python_callable=test_function,
        dag=dag
    )

    train_lstm = PythonOperator(
        task_id='Train_LSTM',
        python_callable=test_function,
        dag=dag
    )

    train_mlp = PythonOperator(
        task_id='Train_MLP',
        python_callable=test_function,
        dag=dag
    )

    train_kmeans = PythonOperator(
        task_id='Train_kmeans',
        python_callable=test_function,
        dag=dag
    )

    select_model = PythonOperator(
        task_id='Select_best_model',
        python_callable=test_function,
        dag=dag
    )

    save_model = PythonOperator(
        task_id='Save_models',
        python_callable=test_function,
        dag=dag
    )



    extract >> anomaly >> prediction_prepare >> [train_lstm, train_mlp] >> select_model >> save_model
    extract >> anomaly >> kmeans_prepare >> train_kmeans >> save_model
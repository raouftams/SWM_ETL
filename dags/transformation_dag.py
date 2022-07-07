from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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
    'ETL_dag',
    default_args=default_args,
    description='ETL transformation process',
    schedule_interval=timedelta(days=30)
) as dag:

    #creating tasks
    prepare_data = PythonOperator(
        task_id='structure_data',
        python_callable=test_function,
        dag=dag
    )

    transform_towns = PythonOperator(
        task_id='transform_towns',
        python_callable=test_function,
        dag=dag
    )

    transform_vehicles = PythonOperator(
        task_id='transform_vehicles',
        python_callable=test_function,
        dag=dag
    )

    transform_dates_times = PythonOperator(
        task_id='transform_dates_times',
        python_callable=test_function,
        dag=dag
    )

    transform_ticket_data = PythonOperator(
        task_id='transform_ticket_data',
        python_callable=test_function,
        dag=dag
    )

    join_data = PythonOperator(
        task_id='join_data',
        python_callable=test_function,
        dag=dag
    )

    extract_data = PythonOperator(
        task_id='Extract_data',
        python_callable=test_function,
        dag=dag
    )

    load_new_vehicles = PythonOperator(
        task_id='load_new_vehicles',
        python_callable=test_function,
        dag=dag
    )

    load_tickets = PythonOperator(
        task_id='load_tickets',
        python_callable=test_function,
        dag=dag
    )

    load_rotations = PythonOperator(
        task_id='load_rotations',
        python_callable=test_function,
        dag=dag
    )


    extract_data >> prepare_data >> [transform_towns, transform_vehicles, transform_dates_times, transform_ticket_data]>> join_data >> [load_new_vehicles, load_tickets] >> load_rotations 
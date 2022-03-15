from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import sys, os


#adding project directory to path
sys.path.append(r"/home/tamssaout/etl")
sys.path.append(r"/home/tamssaout/etl/config")
import petl as etl
from extract import extract_data_from_file
from load import load_rotations
from transform import transform_rotation_data
from utilities import savePkl, openPkl
from config.database import connect


def extract_data():
    print("yelha")
    dir_path = "/home/tamssaout/etl/input/"
    data = []
    for file_path in os.listdir(dir_path):
        path = dir_path + file_path
        print(path)
        data, sheets = extract_data_from_file(path)
    
    print(data)
    savePkl(data, "data.pkl", dir_path)


def transform_data(ti):
    data = openPkl("/home/tamssaout/etl/input/data.pkl")
    print(data)
    print("bien")
    try:
        sheets = data.keys()
    except:
        sheets = None
    table = transform_rotation_data(data, sheets)
    etl.topickle(table, "/home/tamssaout/etl/out/"+date.today().strftime("%d-%m-%Y")+".pkl")


def load_data():
    print(etl.frompickle("/home/tamssaout/etl/out/"+date.today().strftime("%d-%m-%Y")+".pkl"))

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
    'rotation_files_dag',
    default_args=default_args,
    description='Rotation data etl',
    schedule_interval=timedelta(days=30)
) as dag:

    #creating tasks
    extract_rotation_file = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag
    )

    transform_rotation_file = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag
    )

    load_rotation_file = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag
    )

    extract_rotation_file >> transform_rotation_file >> load_rotation_file
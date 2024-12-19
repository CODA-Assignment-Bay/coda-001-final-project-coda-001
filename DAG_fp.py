from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt
dags_path = '/home/user/airflow/dags/'

# Setup configuration for default argument
default_args = {
    'owner': 'felix',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

# Setup Dag
with DAG('dummy_test_final_project',
         description='ETL Process automation pipeline for dummy final project',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * 6',
         ) as dag:

    extractData = BashOperator(task_id='extract_data',
                                 bash_command= f'python3 {dags_path}extract_fp.py')
    
    transformData = BashOperator(task_id='transform_data',
                                 bash_command= f'python3 {dags_path}transform_fp.py')

    loadData = BashOperator(task_id='load_data',
                                 bash_command= f'python3 {dags_path}load_fp.py')


extractData >> transformData >> loadData
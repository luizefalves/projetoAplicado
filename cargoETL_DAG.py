from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_parallel_dag import subdag_parallel_dag
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from extraction import main as main_extraction
from webscrapping import main as main_webscrapping

def _read_api():  
    main_extraction()

def _read_scrapping():  
    main_webscrapping()

def _read_processing():
    return 

default_args = {
    'start_date': datetime(2020,1,1)
}


with DAG('cargoETL_dag', schedule_interval='@daily',default_args=default_args , catchup=False) as dag:
    #Define the tasks/operators

    task_1 = PythonOperator(
    task_id='read_api',
    python_callable= _read_api
    )

    task_2 = PythonOperator(
    task_id='web_scrapping',
    python_callable= _read_scrapping
    )


    task_3 = PythonOperator(
    task_id='emr_processing',
    python_callable= _read_processing
    )


    
task_1 >> task_2 >> task_3

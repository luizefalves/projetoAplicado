from airflow.models import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from extraction import main as main_extraction
from webscrapping import main as main_webscrapping
from processing import main as main_processing

def _read_api():  
    main_extraction()
    return
    
def _read_scrapping():  
    main_webscrapping()
    return

def _read_processing():
    main_processing()
    

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

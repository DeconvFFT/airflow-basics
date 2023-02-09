from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'DeconvFFT',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
    
}
def greet(task_instance):
    first_name = task_instance.xcom_pull(task_ids = 'get_name', key='first_name') # pulls a value from task with id get_name, with key mentioned
    last_name = task_instance.xcom_pull(task_ids = 'get_name', key='last_name') # pulls a value from task with id get_name, with key mentioned
    age = task_instance.xcom_pull(task_ids = 'get_age', key='age') # pulls a value from task with id get_age, with key mentioned

    print(f'Greeting Earthlings! My name is {first_name} {last_name},'
    f'and I am {age} years old')

def get_name(task_instance):
    task_instance.xcom_push(key='first_name', value='Jane')
    task_instance.xcom_push(key='last_name', value='Doe')

def get_age(task_instance):
    task_instance.xcom_push(key='age', value=25)

with DAG(
    default_args=default_args,
    dag_id='python_operator_v0.3',
    description='First DAG with python operator',
    start_date=datetime(2023,2,3),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )
    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
    )
    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable=get_age
    )

    [task2, task3] >> task1 # task1 is downstream of task2 and task3
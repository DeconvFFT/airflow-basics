from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'DeconvFFT',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}
with DAG(
    dag_id='our_first_dag_v3',
    default_args=default_args,
    description='This is our first DAG that we write',
    start_date = datetime(2021, 7, 29, 2),
    schedule_interval = '@daily'
)as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world, this is our first task!'
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo hello world, this is our second task!'
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo hello world, this is our third task running at the same time as task 2!'
    )
    task1.set_downstream(task2)
    task1.set_downstream(task3)
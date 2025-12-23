# Older Approach

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# from datetime import datetime


def task_1():
    print("Task 1 executed")


def task_2():
    print("Task 2 executed")


def task_3():
    print("Task 3 executed")


with DAG(
    dag_id="simple_python_flow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    t1 = PythonOperator(task_id="task_1", python_callable=task_1)
    t2 = PythonOperator(task_id="task_2", python_callable=task_2)
    t3 = PythonOperator(task_id="task_3", python_callable=task_3)

    t1 >> t2 >> t3

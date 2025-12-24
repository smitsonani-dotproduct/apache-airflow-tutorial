# TaskFlow API ONLY

import pendulum
import os
from airflow import DAG
from airflow.decorators import task


FILE_PATH = "/home/smit/airflow/data/input/sample.txt"


with DAG(
    dag_id="local_file_processing_workflow",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["file", "local"],
) as dag:

    @task()
    def read_file():
        if not os.path.exists(FILE_PATH):
            raise FileNotFoundError(f"{FILE_PATH} not found")
        with open(FILE_PATH, "r") as file:
            data = file.readlines()

        print("file data =>", data)
        numbers = [int(x.strip()) for x in data]
        return numbers  # automatically stored in XCom

    @task()
    def process_data(numbers):
        total = sum(numbers)
        avg = total / len(numbers)

        print("Numbers:", numbers)
        print("Total:", total)
        print("Average:", avg)

    numbers = read_file()
    process_data(numbers)

"""
=========== PythonOperator ONLY (Old Style)

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import os

FILE_PATH = "/home/<username>/airflow/data/input/sample.txt"

def read_file(**context):
    with open(FILE_PATH) as f:
        numbers = [int(x.strip()) for x in f.readlines()]
    context["ti"].xcom_push(key="numbers", value=numbers)

def process_data(**context):
    numbers = context["ti"].xcom_pull(
        task_ids="read_file", key="numbers"
    )
    print("Total:", sum(numbers))
    print("Average:", sum(numbers) / len(numbers))

with DAG(
    dag_id="local_file_processing_workflow",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
):

    t1 = PythonOperator(
        task_id="read_file",
        python_callable=read_file,
    )

    t2 = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    t1 >> t2
    
========
"""

"""
Tasks:

Task 1: read data from file
Task 2: process data 

"""

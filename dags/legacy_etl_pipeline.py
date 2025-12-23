import json
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def extract():
    data_string = '{"1000": 301.27, "1002": 433.21, "1003":502.22}'
    return json.loads(data_string)


def transform(ti):  # ti - task instances
    order_data_dict = ti.xcom_pull(
        task_ids="extract"
    )  # XComs – Passing Data Between Tasks. This is how tasks pass metadata/data to each other.
    total_order_value = sum(order_data_dict.values())
    return {"total_order_value": total_order_value}


def load(ti):  # ti - task instance
    total = ti.xcom_pull(task_ids="transform")["total_order_value"]
    print(f"Total order value is: {total:.2f}")


with DAG(
    dag_id="legacy_etl_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    description="Legacy ETL pipeline for sales data",
) as tag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task

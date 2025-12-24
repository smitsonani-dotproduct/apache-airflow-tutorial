from datetime import date, datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# Create Users table into db
def create_users_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql_query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            firstname VARCHAR(50),
            lastname VARCHAR(50),
            nickname VARCHAR(50),
            gender VARCHAR(20),
            age INTEGER CHECK(age >= 0),
            email VARCHAR(50) UNIQUE NOT NULL,
            mobile VARCHAR(15),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    hook.run(sql=sql_query)


# Insert data into db
def insert_users_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql_query = """
        INSERT INTO users (
            firstname, lastname, nickname, gender, age, email, mobile
        )
        VALUES
            ('Smit', 'Sonani', 'ss', 'Male', 20, 'smit@example.com', '+91-1030504001'),
            ('Nensi', 'Mangukiya', 'nm', 'Female', 45, 'nensi@example.com', '+91-4001020030'),
            ('Kamal', 'Hasan', 'ks', 'Male', 30,'kamal@example.com', '+91-9190909190'),
            ('Maya', 'Karadiya', 'mk', 'Female', 34, 'maya@example.com', '+91-9300930010'),
            ('Rahul', 'Patel', 'rp', 'Male', 18, 'rahul@example.com', '+91-7654300000')
        ON CONFLICT (email) DO NOTHING;
        """
    hook.run(sql=sql_query)


# Fetch data from db
def fetch_users_to_dataframe():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
    SELECT *
    FROM users
    WHERE age BETWEEN 20 AND 40;
    """

    df = hook.get_pandas_df(sql=sql)

    print("users data :)", df)
    print(f"Total rows fetched: {len(df)} :)")

    temp_path = "/tmp/users.parquet"
    df.to_parquet(temp_path, index=False)

    print(f"Fetched {len(df)} rows and stored at {temp_path} :)")

    return {"path": temp_path, "processed_rows": len(df)}


# Save into CSV file
def save_csv(**context):
    import pandas as pd

    data = context["ti"].xcom_pull(task_ids="fetch_users_to_dataframe")
    print("input data :)", data)

    logical_date = context["logical_date"]
    formatted_date = logical_date.format("YYYYMMDD")

    df = pd.read_parquet(data["path"])
    csv_path = f"/home/smit/airflow/data/output/users-{formatted_date}.csv"

    df.to_csv(csv_path, index=False)


# Add Summary data into db
def insert_summary_to_db(**context):
    data = context["ti"].xcom_pull(task_ids="fetch_users_to_dataframe")
    print("input data :)", data)

    processed_rows = data["processed_rows"]
    summary_text = f"{processed_rows} rows processed"
    print("summary_text :)", summary_text)

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # create summary table
    hook.run(
        """
        CREATE TABLE IF NOT EXISTS summary (
            id SERIAL PRIMARY KEY,
            run_date DATE NOT NULL,
            summary TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Insert row with summary
    hook.run(
        """
        INSERT INTO summary (run_date, summary)
        VALUES (%s, %s);
        """,
        parameters=(date.today(), summary_text),
    )


with DAG(
    dag_id="example_task",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    from airflow.providers.standard.operators.python import PythonOperator

    create_table_task = PythonOperator(
        task_id="create_users_table",
        python_callable=create_users_table,
    )

    insert_data_task = PythonOperator(
        task_id="insert_users_data",
        python_callable=insert_users_data,
    )

    fetch_users_task = PythonOperator(
        task_id="fetch_users_to_dataframe",
        python_callable=fetch_users_to_dataframe,
    )

    save_csv_task = PythonOperator(task_id="save_csv", python_callable=save_csv)

    insert_summary_to_db_taks = PythonOperator(
        task_id="insert_summary_to_db", python_callable=insert_summary_to_db
    )

    (
        create_table_task
        >> insert_data_task
        >> fetch_users_task
        >> [save_csv_task, insert_summary_to_db_taks]
    )

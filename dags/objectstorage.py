"""
→ Call Air Quality API
→ Convert JSON → Pandas DataFrame
→ Store data as Parquet in S3 (Object Storage)
→ Read Parquet using DuckDB
→ Analyze / print data
"""

from collections.abc import Mapping
import pendulum
import requests
from airflow.sdk import ObjectStoragePath, dag, task
from airflow.models import Variable

API = Variable.get("AIR_QUALITY_API_KEY")
BUCKET_PATH = Variable.get("S3_AIRFLOW_BUCKET_PATH")
print("API :)", API)
print("Bucket path :)", BUCKET_PATH)

base = ObjectStoragePath(BUCKET_PATH)
aq_fields = {
    "pm10": "float64",
    "pm2_5": "float64",
    "carbon_monoxide": "float64",
    "nitrogen_dioxide": "float64",
    "sulphur_dioxide": "float64",
    "ozone": "float64",
    "european_aqi": "float64",
    "us_aqi": "float64",
}


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def objectstorage():

    @task
    def get_air_quality_data(**kwargs) -> ObjectStoragePath:
        import pandas as pd

        print("kwargs :)", kwargs)

        logical_date = kwargs["logical_date"]

        latitude = 28.6139
        longitude = 77.2090

        params: Mapping[str, str | float] = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(aq_fields.keys()),
            "timezone": "UTC",
        }

        response = requests.get(API, params=params)
        response.raise_for_status()

        data = response.json()
        print("API response :)", data)
        hourly_data = data.get("hourly", {})

        df = pd.DataFrame(hourly_data)

        df["time"] = pd.to_datetime(df["time"])

        # ensure the bucket exists
        base.mkdir(exist_ok=True)

        formatted_date = logical_date.format("YYYYMMDD")
        path = base / f"air_quality_{formatted_date}.parquet"

        with path.open("wb") as file:
            df.to_parquet(file)

        print("storage path :)", path)
        return path

    @task
    def analyze(
        path: ObjectStoragePath,
    ):
        import duckdb

        conn = duckdb.connect(database=":memory:")
        conn.register_filesystem(path.fs)
        s3_path = path.path
        print("s3_path :)", s3_path)

        conn.execute(
            f"CREATE OR REPLACE TABLE airquality_urban AS SELECT * FROM read_parquet('{path.protocol}://{s3_path}')"
        )

        df2 = conn.execute("SELECT * FROM airquality_urban").fetchdf()

        print("FINAL DATA :)", df2.head())

    obj_path = get_air_quality_data()
    analyze(obj_path)


objectstorage()

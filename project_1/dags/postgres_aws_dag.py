import os
import requests
import pandas as pd
from datetime import datetime

from airflow.decorators import dag, task
from utils import S3_CLIENT, get_postgres_engine

# Default arguments for DAG
default_args = {"start_date": datetime(2025, 10, 13)}

# SQL to create view
SQL_QUERY = """ 
CREATE OR REPLACE VIEW titanic_count_survivors AS
SELECT
    "Sex",
    SUM("Survived") as survivors_count
FROM titanic
GROUP BY "Sex";
"""

@dag(
    dag_id="postgres_aws_taskflow_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["titanic"],
)
def postgres_aws_dag():

    @task
    def download_data():
        """Download Titanic dataset from a public URL."""
        destination = "/tmp/titanic.csv"
        url = (
            "https://raw.githubusercontent.com/neylsoncrepalde/"
            "titanic_data_with_semicolon/main/titanic.csv"
        )
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(destination, mode="wb") as f:
            f.write(response.content)
        return destination

    @task
    def write_to_postgres(source):
        """Write CSV to Postgres and create a view."""
        df = pd.read_csv(source, sep=";")
        engine = get_postgres_engine()

        # Write table
        df.to_sql(
            "titanic",
            engine,
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi"
        )

        # Create view
        with engine.connect() as conn:
            conn.execute(SQL_QUERY)

    @task
    def upload_to_s3(source):
        """Upload CSV file to S3 bucket."""
        S3_CLIENT.upload_file(source, "titanic-test-csv", "titanic.csv")
        return f"s3://titanic-test-csv/titanic.csv"

    # DAG task flow
    csv_file = download_data()
    write = write_to_postgres(csv_file)
    upload = upload_to_s3(csv_file)


execution = postgres_aws_dag()

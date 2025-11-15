import os

from sqlalchemy import create_engine

def get_postgres_engine():
    return create_engine(os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"))
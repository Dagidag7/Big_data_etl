import subprocess
import sys
from prefect import flow, task

@task
def extract():
    subprocess.run([sys.executable, "scripts/extract_data.py"], check=True)

@task
def transform():
    subprocess.run([sys.executable, "scripts/transform_data.py"], check=True)

@task
def load():
    subprocess.run([sys.executable, "scripts/load_data.py"], check=True)

@flow(name="ETL Pipeline")
def etl_pipeline():
    extract()
    transform()
    load()

if __name__ == "__main__":
    etl_pipeline()
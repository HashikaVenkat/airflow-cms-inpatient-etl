from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import zipfile
import os
import shutil

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 1),
    "retries": 1,
}

DAG_ID = "cms_inpatient_download_unzip"

#Data directories inside the Airflow container
BASE_DIR = "/opt/airflow/data"
ZIP_DIR = os.path.join(BASE_DIR, "zipped_data")
CSV_DIR = os.path.join(BASE_DIR, "csv_data")
#calling API
ZIP_URL = "https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_to_2010_inpatient_claims_sample_1.zip"

ZIP_PATH = os.path.join(ZIP_DIR, "inpatient_claims.zip")
TMP_EXTRACT_DIR = "/tmp/inpatient_claims_extract"
#task 1: Downloading CMS inpateint claims ZIP fie to local Airflow data volume
def task1_download_zip_local():
    os.makedirs(ZIP_DIR, exist_ok=True)

    r = requests.get(ZIP_URL, stream=True, timeout=120)
    r.raise_for_status()

    with open(ZIP_PATH, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    if not os.path.exists(ZIP_PATH) or os.path.getsize(ZIP_PATH) == 0:
        raise ValueError("Downloaded ZIP is missing or empty")
#Extracting the downloaded ZIP file and satging CSV Files for downstream processing
def task2_unzip_to_local():
    os.makedirs(CSV_DIR, exist_ok=True)

    if not os.path.exists(ZIP_PATH):
        raise FileNotFoundError(f"ZIP not found at {ZIP_PATH}. Run Task 1 first.")

    if os.path.exists(TMP_EXTRACT_DIR):
        shutil.rmtree(TMP_EXTRACT_DIR)
    os.makedirs(TMP_EXTRACT_DIR, exist_ok=True)

    with zipfile.ZipFile(ZIP_PATH, "r") as z:
        z.extractall(TMP_EXTRACT_DIR)

    extracted_files = os.listdir(TMP_EXTRACT_DIR)
    if not extracted_files:
        raise ValueError("ZIP extracted, but no files found inside.")

    for name in extracted_files:
        src = os.path.join(TMP_EXTRACT_DIR, name)
        dst = os.path.join(CSV_DIR, name)
        if os.path.exists(dst):
            os.remove(dst)
        shutil.move(src, dst)

    shutil.rmtree(TMP_EXTRACT_DIR)

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Download and extract CMS inpatient claims data locally",
    schedule=None,
    catchup=False,
) as dag:

    download_cms_inpatient_zip = PythonOperator(
        task_id="download_cms_inpatient_zip",
        python_callable=task1_download_zip_local,
    )

    extract_cms_inpatient_zip = PythonOperator(
        task_id="extract_cms_inpatient_zip",
        python_callable=task2_unzip_to_local,
    )

    download_cms_inpatient_zip >> extract_cms_inpatient_zip

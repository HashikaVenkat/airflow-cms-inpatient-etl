from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import json
import os


# ---- Default settings for all tasks in this DAG ----
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---- What happens if a task fails ----
def failure_callback(context):
    # Just print the task id that failed to the log
    print(f"Task {context['task_instance'].task_id} failed")


# ---- Define the DAG itself ----
@dag(
    default_args=default_args,
    description="A simple DAG to pull data from an API",
    schedule_interval=timedelta(days=1),   # run once a day
    start_date=datetime(2024, 8, 1),
    catchup=False,
)
def simple_api_dag():

    # ---- First task: call the API and save data to a file INSIDE the container ----
    @task(on_failure_callback=failure_callback)
    def pull_data_from_api():
        url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?crash_date=2014-01-21T00:00:00.000"
        response = requests.get(url)

        if response.status_code != 200:
            raise ValueError("Failed to pull data from API")

        data = response.json()

        # Save file to /tmp in the Airflow container
        json_path = "/opt/airflow/dags/api_data.json"
        with open(json_path, "w") as f:
            json.dump(data, f)

        print(f"Pulled {len(data)} records from the API")
        print(f"Saved data to {json_path}")

    # ---- Second task: just print 'Hello, World!' using Bash ----
    t1 = pull_data_from_api()

    t2 = BashOperator(
        task_id="echo_hello_world",
        bash_command='echo "Hello, World!"',
    )

    # Say: run t1 first, then t2
    t1 >> t2


# This line actually creates the DAG object that Airflow will see
dag = simple_api_dag()


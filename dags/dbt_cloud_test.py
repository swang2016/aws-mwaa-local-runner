from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from dbt_cloud_utils import dbt_cloud_job_runner


dag_file_name = __file__

# TODO: MANUALLY create a dbt Cloud job: https://docs.getdbt.com/docs/dbt-cloud/cloud-quickstart#create-a-new-job
# Example dbt Cloud job URL
# https://cloud.getdbt.com/#/accounts/4238/projects/12220/jobs/12389/
# example dbt Cloud job config
dbt_cloud_job_runner_config = dbt_cloud_job_runner(
    account_id=3914, project_id=5042, job_id=29720, cause=dag_file_name
)


default_args = {
    "owner": "airflow",
    "email": ["airflow@example.com"],
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 21),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "priority_weight": 1000,
}


with DAG(
    "dbt_cloud_example", default_args=default_args, schedule_interval="@once"
) as dag:
    # Single task to execute dbt Cloud job and track status over time
    adhoc_job = PythonOperator(
        task_id="run-dbt-cloud-job",
        python_callable=dbt_cloud_job_runner_config.run_job,
        provide_context=True,
    )

    adhoc_job

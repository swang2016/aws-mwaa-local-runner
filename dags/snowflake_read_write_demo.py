from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from airflow.models import Variable

db_user = Variable.get("db_user")
db_password = Variable.get("db_password")
db_account = Variable.get("db_account")

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

prod_url = URL(
    account = db_account,
    user = db_user,
    password = db_password,
    database = 'WAREHOUSE',
    schema = 'ANALYTICS',
)
prod_engine = create_engine(prod_url)
prod_connection = prod_engine.connect()

dev_url = URL(
    account = db_account,
    user = db_user,
    password = db_password,
    database = 'DEV',
    schema = 'STEVEN',
)
dev_engine = create_engine(dev_url)
dev_connection = dev_engine.connect()


sql = '''
select
    ref,
    submitted_at_et
from fact_applications
where year(submitted_at_et) = 2022
and month(submitted_at_et) >= 3'''

def read_transform_snowflake_table(sql, target_table_name, target_schema, read_connection, write_connection):
    df = pd.read_sql(sql, read_connection)
    df['new_col'] = 1
    df.to_sql(name = target_table_name, con = write_connection, schema = target_schema, if_exists = 'replace', index = False)



# Using the context manager alllows you not to duplicate the dag parameter in each operator
with DAG('snowflake_demo', default_args=default_args, schedule_interval='@once') as dag:
    read_transform_snowflake_table = PythonOperator(
        task_id='read_transform_snowflake_table',
        python_callable=read_transform_snowflake_table,
        op_kwargs = {
            'sql':sql,
            'target_table_name':'airflow_testing',
            'target_schema':'STEVEN',
            'read_connection':prod_connection,
            'write_connection':dev_connection
        }
    )

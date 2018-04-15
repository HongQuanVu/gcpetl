from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook, BigQueryCursor, BigQueryConnection
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_download_operator import *
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from datetime import date
from datetime import datetime
from datetime import timedelta
from os import environ
from google.cloud import bigquery
import time
import psycopg2
from airflow.dags.gcpetl.gcp_utils import *
from airflow.dags.gcpetl.gcp_airflow_interfaces import bq_execute_query_op


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.combine(date.today(), datetime.min.time()) - timedelta(1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

bq_credential_file_path = "/Users/apple/Downloads/workshopbi01-bq-service-account.json"
RETRIES = 1
RETRY_DELAY = 1

dt = datetime.today()

midnight = datetime.combine(dt.date(), datetime.min.time())

start_business_date = midnight + \
                      timedelta(hours=6, minutes=0) - timedelta(days=1)

schedule_interval = timedelta(days=1)

timeout_interval = timedelta(minutes=60 * 3)

# If unable to send email within an hour , notify

email_sla = timedelta(hours=1)

execution_date = """'{{ ds }}'"""  # '2017-02-01'
dv_yyyy_mm_dd = "{{ ds }}"  # 2017-02-01
dv_yyyymmdd = "{{ ds_nodash }}"  # 20170201

RETRIES = 1
RETRY_DELAY = timedelta(seconds=1)
SLA = timedelta(minutes=30)
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_business_date,
    'email': ['quanvu@chotot.vn'],
    'email_on_failure': True,
    'email_on_retry': True,
    'sla': timedelta(minutes=30),
    'retries': 60 * 10,
    'retry_delay': timedelta(minutes=2)
}

dag_id = 'DEMO_RUN_DML_Statement'
dag = DAG(
        dag_id=dag_id,
        default_args=args,
        schedule_interval=schedule_interval,
        dagrun_timeout=timeout_interval,
        start_date=start_business_date
)

#globals()[dag_id] = dag
sql_command = "delete FROM `workshopbi01-200202.bi_acct_prop_data.AA` WHERE business_date =  $BUSINESS_DATE "

PythonOperator(
        dag=dag, task_id="Delete_Old_Data",
        provide_context=True,
        python_callable=bq_execute_query_op,
        retries=RETRIES,
        retry_delay=RETRY_DELAY,
        op_kwargs={
            'bigquery_credentials_file_path': bq_credential_file_path,
            'source_sql': sql_command
        }
    )

if __name__ == "__main__":
    print ("load DAG..")
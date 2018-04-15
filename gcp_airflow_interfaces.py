import logging
from  airflow.dags.gcpetl.gcp_utils import *
from datetime import date
from datetime import datetime
from datetime import timedelta
from google.cloud import bigquery
from os import environ

SQL_SEPARATOR = ";"
SCHEMA_NAME_TABLE_NAME_SEPARATOR = "."
STANDARD_SQL_NOTE = "#standard sql"

def bq_execute_query_op(bigquery_credentials_file_path, source_sql ,use_legacy_sql=False,**kwargs) :
    business_date = datetime.strftime(kwargs['execution_date'], "%Y-%m-%d")
    yyyy_mm_dd=  business_date
    yyymmdd = datetime.strftime(kwargs['execution_date'], "%Y%m%d")
    source_sql = source_sql.replace("$BUSINESS_DATE", "'%s'" % (business_date)) #replace $BUSINESS_DATE by 'YYYY-MM-DD'
    source_sql = source_sql.replace("$YYYYMMDD", "%s" % (yyymmdd))
    source_sql = source_sql.replace("$YYYY-MM-DD", "%s" % (yyyy_mm_dd))         #replace $YYYY-MM-DD by YYYY-MM-DD
    environ["GOOGLE_APPLICATION_CREDENTIALS"] = bigquery_credentials_file_path
    source_sql = source_sql.replace("$BUSINESS_DATE", "%s" % (business_date))
    source_sql = source_sql.replace("$YYYYMMDD", "%s" % (yyymmdd))
    logging.info(source_sql)
    client = bigquery.Client()
    bq_execute_query(google_client=client,query=source_sql)


import os
import logging

from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from airflow.hooks.base_hook import BaseHook

def tables_to_bronze(table, **kwargs):
    
    logging.info(f"Loading table {table} to bronze")
    ds = kwargs.get('ds',str(date.today()))
    
    pg_conn = BaseHook.get_connection('Postgres_HT14')
    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
    pg_creds = {"user": pg_conn.login,"password":pg_conn.password}
    
    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.3.1.jar')\
        .master('local')\
        .appName('HT14_tables_to_bronze')\
        .getOrCreate()
        
    table_df = spark.read.jdbc(pg_url,table=table,properties = pg_creds)
    table_df.select([F.col(c).cast("string") for c in table_df.columns])
    table_df.write.option('header','true').csv(
        os.path.join('/','datalake','bronze','dshop',table,ds),
        mode='overwrite')
    
    logging.info(f"Loading table {table} to bronze Completed")
    
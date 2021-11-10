import os
import logging

from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def tables_to_silver(table, **kwargs):
    
    logging.info(f"Loading table {table} to silver")
    ds = kwargs.get('ds',str(date.today()))
    

    
    spark = SparkSession.builder\
        .master('local')\
        .appName('HT14_tables_to_silver')\
        .getOrCreate()
        
    
    logging.info(f"Loading table {table} from bronze")  
    table_df = spark.read\
        .option('header',True)\
        .option('inferSchema',True)\
        .csv(os.path.join('/','datalake','bronze','dshop',table,ds))
    logging.info(f"Table {table} processing")
    
    table_df.dropDuplicates()
    
    
    logging.info(f"Table {table} writing to silver")
    table_df.write.parquet(
        os.path.join('/','datalake','silver','dshop',table),
        mode = 'overwrite'
        )
       
        
        
    
    logging.info(f"Loading table {table} to silver Completed")
    
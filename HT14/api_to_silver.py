import os
import logging

from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def api_to_silver(**kwargs):
    
    
    ds = kwargs.get('ds',str(date.today()))

    logging.info(f"Loading api data for {ds} to silver")

    
    spark = SparkSession.builder\
        .master('local')\
        .appName('HT14_api_to_silver')\
        .getOrCreate()
        
    #tables=['clients','orders','products','aisles','departments']
    
    logging.info(f"Loading api json {ds} from bronze")  
    table_df = spark.read\
        .json(os.path.join('/','datalake','bronze','api_data',ds,'rd_api_data.json'))
    logging.info(f"Date {ds} processing")
    
    table_df.where(F.col("date")==ds)\
        .withColumn('date', F.col('date').cast("date"))\
        .withColumn('product_id', F.col('product_id').cast("integer"))\
        .where(F.col('product_id').isNotNull())
    
    
    
    logging.info(f"Api data for {ds} writing to silver")
    table_df.write.parquet(
        os.path.join('/','datalake','silver','api_data',ds),
        mode = 'overwrite'
        )
    
    
    logging.info(f"Loading API data for {ds} to silver Completed")
    
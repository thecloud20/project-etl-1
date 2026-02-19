import logging
import pandas as pd

from pyspark.sql import functions as F

from src.utils.spark_session import get_spark_session
from src.utils.schemas import ORDERS_SCHEMA,CUSTOMERS_SCHEMA
from src.utils.config import *


logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

def extract_orders(spark):
    logger.info("reading orders")

    df=spark.read.schema(ORDERS_SCHEMA).csv(ORDERS_PATH,header=True)

    total_rows=df.count()

    df_clean= (
        df
        .withColumn('order_date',F.to_date('order_date'))
        .filter(F.col('unit_price').isNotNull())

        .filter(F.lower(F.col("status")).isin('completed','returned','cancelled'))
        .dropDuplicates(['order_id'])
        .withColumn('extracted_at',F.current_timestamp())
    )
    clean_rows=df_clean.count()
    logger.info(f"Orders Read: {total_rows}")
    logger.info(f"Orders Cleaned: {clean_rows}")

    df_clean.toPandas().to_parquet(STAGING_ORDERS,index=False)



    return df_clean

def extract_products(spark):

    logger.info("Reading Products")

    df = spark.read.option("multiLine",True).json(PRODUCTS_PATH)

    df_clean = (
        df
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("product_name").isNotNull())
        .filter(F.col("cost_price")>0)
        .withColumn("extracted_at",F.current_timestamp())
    )
    df_clean.toPandas().to_parquet(STAGING_PRODUCTS, index=False)



    return df_clean

def extract_customers(spark):

    logger.info("Reading Customers")

    df = spark.read.schema(CUSTOMERS_SCHEMA).csv(CUSTOMERS_PATH, header=True)

    df_clean = (
        df
        .filter(F.col("email").contains("@"))
        .withColumn("join_date", F.to_date("join_date"))
        .withColumn("extracted_at", F.current_timestamp())
    )
    df_clean.toPandas().to_parquet(STAGING_CUSTOMERS, index=False)


    return df_clean

def run_extraction():

    spark = get_spark_session()

    result = {}

    try:
        result["orders"]=extract_orders(spark)
    except Exception as e:
        logger.error(f"Orders Failed {e}")

    try:
        result["products"]= extract_products(spark)
    except Exception as e:
        logger.error(f"Products Failed {e}")

    try:
        result["customers"]= extract_customers(spark)
    except Exception as e:
        logger.error(f"Customers Failed {e}")

    logger.info("Extraction Completed --PROJECT-ETL-1")

    return result

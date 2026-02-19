from pyspark.sql.types import *

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customers_id", StringType()),
    StructField("product_id", StringType()),
    StructField("order_date", StringType()),
    StructField("quantity", IntegerType()),
    StructField("UNIT_PRICE",DoubleType()),
    StructField("region", StringType()),
    StructField("status", StringType()),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("loyalty_tier", StringType()),
    StructField("join_date", StringType())
])
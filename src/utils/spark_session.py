
from pyspark import SparkContext
from pyspark.sql import SparkSession

def get_spark_session():
    spark=(SparkSession.builder
        .appName('project-etl-1')
        .master('local[*]')
        .config('spark.sql.shuffle.partitions','4')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('ERROR')
    return spark





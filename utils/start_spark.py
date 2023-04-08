from pyspark.sql import SparkSession
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
sparkBuilder = SparkSession.builder \
    .master('local[1]') \
    .appName('pbd') \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .config("spark.sql.repl.eagerEval.truncate", 50) \
    .config("spark.sql.repl.eagerEval.maxNumRows", 50) \
    .config("spark.driver.memory","4g") \
    .config("spark.executer.memory","4g")
spark: SparkSession = sparkBuilder.getOrCreate()


def reload(spark):
    spark.stop()
    spark = sparkBuilder.getOrCreate()
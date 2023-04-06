from pyspark.sql import SparkSession

sparkBuilder = SparkSession.builder \
    .master('local[1]') \
    .appName('pbd') \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .config("spark.sql.repl.eagerEval.truncate", 50) \
    .config("spark.sql.repl.eagerEval.maxNumRows", 50)
spark: SparkSession = sparkBuilder.getOrCreate()

def reload(spark):
    spark.stop()
    spark = sparkBuilder.getOrCreate()
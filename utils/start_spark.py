from pyspark.sql import SparkSession

sparkBuilder = SparkSession.builder \
                    .master('local[1]') \
                    .appName('pbd')
spark: SparkSession = sparkBuilder.getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.truncate", 50)
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 50)

def reload(spark):
    spark.stop()
    spark = sparkBuilder.getOrCreate()
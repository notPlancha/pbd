from pyspark.sql.types import *

train = StructType([StructField('session_id', LongType(), True),
                     StructField('index', IntegerType(), True),
                     StructField('elapsed_time', IntegerType(), True),
                     StructField('event_name', StringType(), True),
                     StructField('name', StringType(), True),
                     StructField('level', IntegerType(), True),
                     StructField('page', IntegerType(), True),
                     StructField('room_coor_x', DoubleType(), True),
                     StructField('room_coor_y', DoubleType(), True),
                     StructField('screen_coor_x', IntegerType(), True),
                     StructField('screen_coor_y', IntegerType(), True),
                     StructField('hover_duration', IntegerType(), True),
                     StructField('text', StringType(), True),
                     StructField('fqid', StringType(), True),
                     StructField('room_fqid', StringType(), True),
                     StructField('text_fqid', StringType(), True),
                     StructField('fullscreen', BooleanType(), True),
                     StructField('hq', BooleanType(), True),
                     StructField('music', BooleanType(), True),
                     StructField('level_group', StringType(), True)])

test = StructType([StructField('session_id', LongType(), True),
                     StructField('index', IntegerType(), True),
                     StructField('elapsed_time', IntegerType(), True),
                     StructField('event_name', StringType(), True),
                     StructField('name', StringType(), True),
                     StructField('level', IntegerType(), True),
                     StructField('page', DoubleType(), True),
                     StructField('room_coor_x', DoubleType(), True),
                     StructField('room_coor_y', DoubleType(), True),
                     StructField('screen_coor_x', DoubleType(), True),
                     StructField('screen_coor_y', DoubleType(), True),
                     StructField('hover_duration', DoubleType(), True),
                     StructField('text', StringType(), True),
                     StructField('fqid', StringType(), True),
                     StructField('room_fqid', StringType(), True),
                     StructField('text_fqid', StringType(), True),
                     StructField('fullscreen', StringType(), True),
                     StructField('hq', IntegerType(), True),
                     StructField('music', IntegerType(), True),
                     StructField('level_group', StringType(), True),
                     StructField('session_level', StringType(), True)])

train_labs = StructType([StructField('session_id', StringType(), True),
                     StructField('correct', IntegerType(), True)])

sample_sub = StructType([StructField('session_id', StringType(), True),
                     StructField('correct', IntegerType(), True)])
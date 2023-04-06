from utils.imports import *

class the_transformer(Transformer):
    def __init__(self, *stages: list[Callable | tuple[Callable, ...]]):
        self.stages = stages
    def _transform(self, df: DataFrame) -> DataFrame:
        dfRet = df
        for i in self.stages:
            if isinstance(i, Iterable):
                dfRet = dfRet.transform(*i)
            else:
                dfRet = dfRet.transform(i)
        return dfRet
    
    
cnt_cond = lambda cond: sum(when(cond, 1).otherwise(0))
def reorder(df, *columns_to_front):
    original = df.columns
    # Filter to present columns
    columns_to_front = [c for c in columns_to_front if c in original]
    # Keep the rest of the columns & sort it for consistency
    columns_other = list(set(original) - set(columns_to_front))
    columns_other.sort()
    # Apply the order
    df = df.select(*columns_to_front, *columns_other)
    return df

def add_id(df):
    return df.withColumn(
        "id",
        row_number().over(Window.orderBy(monotonically_increasing_id()))-1
    )
def elapsed_to_hours(df, old_name, new_name):
    return df.withColumn(new_name, col(old_name)/3.6e6)

def negative_to_0(df, column):
    return df.withColumn(column, when(col(column) < 0, 0).otherwise(col(column)))

def agg1(df):
    return df.groupby('session_id', 'event_name', 'level_group') \
        .agg(
            max(col("index")).alias("max_index_to_sum"),
            max(col("elapsed_time_h")).alias("total_time_h"),
            # avg(
            #     when(
            #         col("diff_elapsed_per_event_ms") != 0,
            #         col("diff_elapsed_per_event_ms")
            #     )
            # ).alias("avg diff elapsed ms/event"),
            cnt_cond(
                (col("event_name") == "observation_click") &
                (col("fqid").isin([
                    "lockeddoor", "coffee", "block_magnify", "photo"
                ]))
            ).alias("obs_opcional_to_sum"),
            cnt_cond(
                (col("event_name") == "observation_click") &
                (col("fqid").isin(["outtolunch", "janitor"]))
            ).alias("obs_no_in_to_sum"),
            cnt_cond(
                (col("event_name") == "observation_click") &
                (col("fqid") != "block_magnify") &
                ((col("fqid") == "need_glasses") | (col("fqid").like("%block%")))
            ).alias("obs_block_to_sum"),
            # aqui podia estar também o map mas é preciso mais análise para determinar qual é o errado
            cnt_cond(
                (col("event_name") == "notebook_click") & 
                (col("name") == "open")
            ).alias("notebook_opens_to_sum"),
            cnt_cond(
                (col("event_name") == "notebook_click") & 
                (col("name").isin(["prev", "next"]))
            ).alias("notebook_explorer_to_sum"),
            cnt_cond(
                (col("event_name") == "notebook_click") & 
                (col("name").isin(["prev", "next", "basic"]))
            ).alias("notebook_clicks"),
            collect_list(col("text")).alias("texts_to_treat"), # to check what type it is
            avg(col("elapsed_diff_ms")).alias("avg_elapsed_diff_ms"),
            avg(col("hover_duration")).alias("avg_hover"),
            first(col("fullscreen"), ignorenulls=True).alias("fullscreen"),
            first(col("hq"), ignorenulls=True).alias("hq"),
            first(col("music"), ignorenulls=True).alias("music"),
        )
def agg2(df):
    return df.groupby("session_id").agg(
        max("max_index_to_sum").alias("max_index"),
        min(when(col("level_group") == "0-4"), 1/col("total_time_h")).alias("inv_total_time_h_0-4"),
        min(when(col("level_group") == "5-12"), 1/col("total_time_h")).alias("inv_total_time_h_5-12"),
        min(1/col("total_time_h")).alias("inv_total_time_h"),
        sum("obs_opcional_to_sum").alias("obs_opcional"),
        sum("obs_no_in_to_sum").alias("obs_no_in"),
        sum("notebook_opens_to_sum").alias("notebook_opens"),
        sum("notebook_explorer_to_sum").alias("notebook_explorer"),
        first(col("fullscreen")).alias("fullscreen"),
        first(col("hq")).alias("hq"),
        first(col("music")).alias("music"),
        avg(col("hover_duration")).alias("avg_hover"),
        
        # TODO missing texts_to_treat, para juntar listas pra uma unica
        # avg_elapsed_diff_ms per event (cutscene, movement and person (i think)),
        # 
def elapsed_to_diff(karr: DataFrame) -> DataFrame:
    lasldlasd = time_paseed(karr.select("elapsed_time"))
    labels_udf = udf(lambda indx: lasldlasd[indx-1], IntegerType())
    return karr.withColumn('elapsed_diff_ms', labels_udf('id'))

def time_paseed(a):
    kard = a.collect()
    lss = 0
    lastos = []
    for i in kard:
        try:
            lastos.append(kard[lss+1][0] - i[0])
        except IndexError:
            lastos.append(0)
        lss+= 1
    return lastos

def elapsed_to_diff_per_group(df: DataFrame, spark) -> DataFrame:
    emp_RDD = spark.sparkContext.emptyRDD()
    columns_ret = df.schema
    columns_ret.add(StructField('diff_elapsed_per_event_ms', IntegerType(), True))
    df_ret = spark.createDataFrame(data = emp_RDD, schema = columns_ret)
    for i in df.select("session_id").distinct().toLocalIterator():
       arraysalvo = time_paseed_group(df.filter(f"session_id == {i[0]}").select("event_name","id_new","elapsed_time"))
       labels_array = F.udf(lambda indx: arraysalvo[indx-1][2], IntegerType())
       df_temp_toAdd = df.filter(f"session_id =={i[0]}").withColumn("id_local",row_number().over(Window.orderBy(monotonically_increasing_id())))
       df_temp_toAdd = df_temp_toAdd.withColumn('diff_elapsed_per_event_ms', labels_array('id_local'))
       df_temp_toAdd = df_temp_toAdd.drop('id_local')
       df_ret.createOrReplaceTempView("fixa")
       df_temp_toAdd.createOrReplaceTempView("tempo")
       df_ret = spark.sql("SELECT * FROM tempo UNION SELECT * FROM fixa")
    return df_ret

def time_passed_group(a):
    kard = a.collect()
    lss = 0
    lastos = []
    for i in kard:
        try:
          if i[0] == kard[lss+1][0]:
            lastos.append([i[0],i[1],kard[lss+1][2] - i[2]])
          else:
            if i[0] == kard[lss-1][0]:
              lastos.append([i[0],i[1],lastos[-1][2]])
            else:
              lastos.append([i[0],i[1],i[2]])
        except IndexError:
            lastos.append([i[0],i[2],lss-lss])
        lss+= 1
    return lastos
        
def typeOfText(df: DataFrame) -> DataFrame:
    raise NotImplementedError()
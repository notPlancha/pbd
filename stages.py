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
            collect_set(col("text")).alias("texts_to_treat"), # to check what type it is
            avg(col("elapsed_diff_ms")).alias("avg_elapsed_diff_ms"),
            # avg(col("hover_duration")).alias("avg_hover_"),
            first(col("fullscreen"), ignorenulls=True).alias("fullscreen"),
            first(col("hq"), ignorenulls=True).alias("hq"),
            first(col("music"), ignorenulls=True).alias("music"),
        )
def agg2(df):
    return df.groupby("session_id").agg(
        max("max_index_to_sum").alias("max_index"),
        min(
            when(
                col("level_group") == "0-4", 
                1/col("total_time_h")
            )
        ).alias("inv_total_time_h_0-4"),
        min(
            when(
                col("level_group") == "5-12", 
                1/col("total_time_h")
            )
        ).alias("inv_total_time_h_5-12"),
        min(1/col("total_time_h")).alias("inv_total_time_h"),
        sum("obs_opcional_to_sum").alias("obs_opcional"),
        sum("obs_no_in_to_sum").alias("obs_no_in"),
        sum("notebook_opens_to_sum").alias("notebook_opens"),
        sum("notebook_explorer_to_sum").alias("notebook_explorer"),
        first(col("fullscreen")).alias("fullscreen"),
        first(col("hq")).alias("hq"),
        first(col("music")).alias("music"),
        # avg(col("avg_hover_")).alias("avg_hover"), #with this method it will make the average per event per level_group then an avg of that. that is fine for me, and maybe even preferable
        collect_set(col("texts_to_treat")).alias("sets_of_texts_to_get_type"),
        avg(
            when(
                col("event_name") == "cutscene_click",
                col("avg_elapsed_diff_ms")
            )
        ).alias("avg_elapsed_diff_ms_cutscene"),
        avg(
            when(
                col("event_name") == "person_click",
                col("avg_elapsed_diff_ms")
            )
        ).alias("avg_elapsed_diff_ms_person"),
        avg(
            when(
                col("event_name") == "navigate_click",
                col("avg_elapsed_diff_ms")
            )
        ).alias("avg_elapsed_diff_ms_navigate")
    )

        
def elapsed_to_diff(df: DataFrame, old, new):
    return df.withColumn(new, col(old) - lag(col(old), offset=1, default=0) \
                         .over(Window.orderBy("id"))
                        )

def typeOfText(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "texts_to_get_type", 
        flatten(col("sets_of_texts_to_get_type"))
    ).withColumn("type_of_script",
        when(
            array_contains(
                col("texts_to_get_type"), "Meetings are BORING!"
            ), "normal"
        ).when(
            array_contains(
                col("texts_to_get_type"), "Sure!"
            ), "dry"
        ).when(
            array_contains(
                col("texts_to_get_type"), "Do I have to?"
            ), "nohumor"
        ).otherwise("noskark")
    ).drop("texts_to_get_type", "sets_of_texts_to_get_type")
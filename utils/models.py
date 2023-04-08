from dataclassy import dataclass, fields, factory
from utils.imports import *
from pyspark.ml.param.shared import *


@dataclass(kwargs=True)
class Model:
  classifier: Type
  train: DataFrame
  test: DataFrame
  features: VectorAssembler
  name: str = ""
  pipeline: Pipeline = None
  fit: Transformer | list[Transformer] = None
  tested: DataFrame = None

  def __post_init__(self, **kwargs):
    if self.name == "": self.name = self.classifier.__name__
    
    stages = [self.features]
    for i in range(1, 19):  # Bin Rel
      model = self.classifier(**kwargs)
      model.setPredictionCol(f"q{i}_pred")
      model.setLabelCol(f"q{i}")
      if model.hasParam("rawPredictionCol"):
        model.setRawPredictionCol(f"q{i}_pred_raw")
      if model.hasParam("probabilityCol") and "probabilityCol" not in kwargs:
        model.setProbabilityCol(f"q{i}_prob")
      if model.hasParam("standardization") and "standardization" not in kwargs:
        model.setStandardization(False)
      if model.hasParam("seed") and "seed" not in kwargs:
        model.setSeed(1)
      stages.append(model)
    self.pipeline = Pipeline(stages=stages)

  def train_and_predict(parquet_file: str = None, write = True):
    if parquet_file is None and write is True: raise ValueError("write is true but no file name provided")
    self.fit = self.pipeline.fit(self.train)
    self.tested = self.fit.transform(self.test)
    if parquet_file is not None:
      self.tested.write.mode("overwrite").parquet(f".\\data\\{parquet_file}")
  
  def load_tested(spark, parquet_file:str):
    self.tested = spark.read.parquet(f".\\data\\{parquet_file}")

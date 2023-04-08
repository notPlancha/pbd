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
  isTested: bool = False
  forEval: DataFrame = None
  
  metrics: dict = None
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


  def train_and_predict(self, write = True):
    self.fit = self.pipeline.fit(self.train)
    self.tested = self.fit.transform(self.test)
    self.isTested = True
    if write:
      self.tested.write.mode("overwrite").parquet(f".\\data\\models\\{self.name}")
  def load_tested(self, spark):
    self.tested = spark.read.parquet(f".\\data\\models\\{self.name}")
    self.isTested = True
  #(subsetAccuracy|accuracy|hammingLoss|precision|recall|f1Measure|precisionByLabel|recallByLabel|f1MeasureByLabel|microPrecision|microRecall|microF1Measure)')
  
  
  def evaluate(self, force=True):
    if not self.isTested and not force: raise ValueError("Model not tested, force to overwite")
    forEval = self.tested
    cols = []
    cols_pred = []
    for i in range(1, 19):
      forEval = forEval.withColumn(f"q{i}", when(col(f"q{i}") == 1, i).otherwise(None))
      forEval = forEval.withColumn(f"q{i}_pred", when(col(f"q{i}_pred") == 1, i).otherwise(None))
      cols.append(f"q{i}")
      cols_pred.append(f"q{i}_pred")
    forEval = forEval.select("session_id", array(*cols).cast("array<double>").alias("label"), array(*cols_pred).cast("array<double>").alias("prediction"))
    self.forEval = forEval.withColumn("prediction", expr("FILTER(prediction, x -> x is not null)")).withColumn("label", expr("FILTER(label, x -> x is not null)"))
    
    met = MultilabelClassificationEvaluator

    
    self.metrics = {
      "subSetAcc": met(metricName = "subsetAccuracy").evaluate(self.forEval), 
      "hammingLoss": met(metricName = "hammingLoss").evaluate(self.forEval), 
      "microF1Measure": met(metricName = "microF1Measure").evaluate(self.forEval),
      "regularF1": met().evaluate(self.forEval)
    }
    
    return self.metrics
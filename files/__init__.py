from dataclasses import dataclass
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

@dataclass
class csvFile:
    path: str
    shema: Optional[str] = None
    _isRead: bool = False
    df: Optional[DataFrame] = None
    def read(self, spark: SparkSession, force = False, infer = None, header = True, **kwargs):
        assert not force and not self._isRead, "Already read, use force = True to overwrite"
        # defaults infer is there's no shema
        if infer is None and self.shema is None: infer = False
        if infer is True or infer is False: 
            self.df = spark.read.csv(self.path, inferSchema = infer, header = header, **kwargs)
        elif self.shema is not None:
            spark.read.csv(self.path, shema = self.shema, header = header, **kwargs)
        self._idRead = True
        return self.df
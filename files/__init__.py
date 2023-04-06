from dataclasses import dataclass
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

@dataclass
class csvFile:
    path: str
    schema: Optional[StructType] = None
    _isRead: bool = False
    df: Optional[DataFrame] = None
    escape: str = "\\"
    def read(self, spark: SparkSession, force = False, allStr = False, infer =False, header = True, **kwargs):
        assert not force and not self._isRead, "Already read, use force = True to overwrite"
        if infer and allStr: raise ValueError("infer and allStr are contraditory")
        if infer or allStr:
            self.df = spark.read.csv(self.path, inferSchema = not allStr, header = header, escape = self.escape, **kwargs)
        elif self.schema is not None:
            self.df = spark.read.csv(self.path, schema = self.schema, header = header, escape = self.escape, **kwargs)
        else: 
            raise ValueError("no schema for csv found, use infer=True or allStr=True if there's none")
        self._isRead = True
        return self.df
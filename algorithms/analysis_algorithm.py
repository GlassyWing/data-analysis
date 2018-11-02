from abc import abstractmethod, ABCMeta

from pyspark.sql import SparkSession, DataFrame


class AnalysisAlgorithm(metaclass=ABCMeta):

    @abstractmethod
    def run(self, df: DataFrame, spark: SparkSession):
        pass

from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F

from algorithms.analysis_algorithm import AnalysisAlgorithm


class PartsDelay(AnalysisAlgorithm):
    """
    零件延误分析
    """

    def __init__(self, delay_column_name="DELAY"):
        self.__delay_column_name = delay_column_name

    def run(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        w_spec = Window.partitionBy("MES_PART_INFO_ID").orderBy("OP_NO")

        condition = F.when(F.isnull((F.unix_timestamp("START_DATE", "yyyy/MM/dd HH:mm:ss") - F.unix_timestamp(
            F.lag("START_DATE", 1).over(w_spec), "yyyy/MM/dd HH:mm:ss"))), 0) \
            .otherwise(
            F.unix_timestamp("START_DATE", "yyyy/MM/dd HH:mm:ss") - F.unix_timestamp(
                F.lag("START_DATE", 1).over(w_spec), "yyyy/MM/dd HH:mm:ss"))

        return df.withColumn(self.__delay_column_name, condition)

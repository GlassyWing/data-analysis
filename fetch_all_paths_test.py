import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from algorithms import FetchAllPaths


def mocked_data(spark: SparkSession):
    rdd = spark.sparkContext.parallelize((
        (1, 1003, 801, 9.0),
        (1, 1019, 801, 12.0),
        (1, 1016, 801, 7.0),
        (1, 1008, 1019, 11.0),
        (1, 1006, 1019, 8.0),
        (1, 1014, 1019, 5.0),
        (1, 1011, 1019, 6.0),
        (1, 1010, 1003, 4.0),
        (1, 1001, 1003, 8.0),
        (1, 1004, 1003, 3.0),
        (1, 1012, 1004, 7.0),
        (1, 1002, 1004, 10.0),
        (1, 1015, 1004, 13.0),
        (2, 1003, 801, 9.0),
        (2, 1019, 801, 12.0),
        (2, 1016, 801, 7.0),
        (2, 1008, 1019, 11.0),
        (2, 1006, 1019, 8.0),
        (2, 1014, 1019, 5.0),
        (2, 1011, 1019, 6.0),
        (2, 1010, 1003, 4.0),
        (2, 1001, 1003, 8.0),
        (2, 1004, 1003, 3.0),
        (2, 1012, 1004, 7.0),
        (2, 1002, 1004, 10.0),
        (2, 1015, 1004, 13.0),
    ))
    return spark.createDataFrame(rdd).toDF("gid", "id", "pid", "cost")


if __name__ == '__main__':
    import os

    os.environ["PYSPARK_PYTHON"] = "C:/Users/14902/Anaconda3/envs/bigdata/python"
    spark = SparkSession.builder.appName("data-analysis") \
        .config("spark.driver.memory", "8g") \
        .master("local[*]") \
        .getOrCreate()

    df = mocked_data(spark)

    alg = FetchAllPaths("id", "pid", weight_name="cost", limit_cols=["gid"])
    result = alg.run(df, spark)
    result.show(50, truncate=False)

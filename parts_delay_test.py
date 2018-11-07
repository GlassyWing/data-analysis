from pyspark.sql import SparkSession, DataFrameReader, DataFrame
import pyspark.sql.functions as F
from algorithms import PartsDelay


def load_delay_data(df_reader: DataFrameReader, spark: SparkSession,
                    mes_part_opplan,
                    mes_part_info_dispatch,
                    mes_part_info):
    df_reader.option("dbtable", mes_part_opplan).load().createOrReplaceTempView(
        "mes_part_opplan")
    df_reader.option("dbtable", mes_part_info_dispatch).load().createOrReplaceTempView(
        "mes_part_info_dispatch")
    df_reader.option("dbtable", mes_part_info).load().createOrReplaceTempView(
        "mes_part_info")

    df = spark.sql('''
          select c.MES_PART_INFO_ID,b.MES_PART_INFO_DISPATCH_ID,a.MES_PART_OPPLAN_ID,
           c.PART_NO,c.LOT_NO,c.FOPLAN_ID,c.TASK_QTY,c.MANUFACTURER,
           b.SCHEDULED_OPERATOR_TYPE,b.SCHEDULED_OPERATOR_NO,a.SCHEDULED_START_DATE,a.SCHEDULED_COMPLETION_DATE,
           a.START_DATE,a.END_DATE,a.OP_NO,a.OP_NAME,a.OP_DESCRIPTION,
           a.OPER_DEPART,a.ACTUAL_MADE_BY,b.COMPLETION_RECORD_CREATOR
          from mes_part_opplan a
          join mes_part_info_dispatch b 
            on a.MES_PART_INFO_ID=b.MES_PART_INFO_ID
            and a.MES_PART_OPPLAN_ID=b.MES_PART_OPPLAN_ID
          join mes_part_info c 
            on a.MES_PART_INFO_ID=c.MES_PART_INFO_ID
        ''')

    return df.filter(F.col("START_DATE").isNotNull())


def save_delay_info(df: DataFrame):
    df.repartition(1) \
        .write \
        .jdbc(url=jdbc_url,
              table="dw.delay_info",
              mode="overwrite",
              properties=connect_properties)


if __name__ == '__main__':
    spark = SparkSession.builder.appName("data-analysis") \
        .config("spark.driver.memory", "8g") \
        .master("local[*]") \
        .getOrCreate()

    # 数据库链接，当作为参数传入
    jdbc_url = "jdbc:postgresql://172.18.130.101/postgres"
    username = "gpadmin"
    password = "gpadmin"

    connect_properties = {"user": username, "password": password, "driver": "org.postgresql.Driver"}

    df_reader = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", username) \
        .option("password", password)

    df = load_delay_data(df_reader, spark, "mes_part_opplan", "mes_part_info_dispatch", "mes_part_info")

    alg = PartsDelay()
    result = alg.run(df, spark)

    # 显示结果
    result.show(50, truncate=False)

    # 保存结果
    # save_delay_info(result)

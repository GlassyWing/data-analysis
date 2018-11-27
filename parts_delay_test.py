import psycopg2 as pg
import psycopg2.extras as E
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrameReader, DataFrame

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


def save_delay_info(df: DataFrame, empty_df: DataFrame):
    # 删除已存在的表，重新创建一个新表
    empty_df.write \
        .jdbc(url=jdbc_url,
              table="dw.delay_info",
              mode="overwrite",
              properties=connect_properties)

    columns = df.columns

    # 保存每一个分区的数据
    df.foreachPartition(lambda rows: save_single_partition(rows, columns))


def save_single_partition(rows, columns):
    """
    保存单个分区的数据
    :param rows:
    :param columns:
    :return:
    """
    columns = list(map(lambda column: '"' + column + '"', columns))  # 必须添加双引号
    db_conn = pg.connect(database="postgres", user="gpadmin", password="gpadmin", host="172.18.130.101", port="5432")
    cursor = db_conn.cursor()
    sql = f'INSERT INTO dw.delay_info ({",".join(columns)}) VALUES ({",".join(["%s"] * len(columns))});'
    E.execute_batch(cursor, sql, rows)
    db_conn.commit()
    cursor.close()
    db_conn.close()


if __name__ == '__main__':
    import os

    os.environ["PYSPARK_PYTHON"] = "C:/Users/14902/Anaconda3/envs/bigdata/python"

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

    empty_result = spark.createDataFrame([], result.schema)

    # 显示结果
    # result.show(50, truncate=False)

    # 保存结果
    save_delay_info(result, empty_result)

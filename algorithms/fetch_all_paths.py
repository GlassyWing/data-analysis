from datetime import datetime

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession

from algorithms import AnalysisAlgorithm


class FetchAllPaths(AnalysisAlgorithm):
    """
    基于Spark SQL的找出树状数据表中的所有路径与其相应的权重（代价）算法

    1. 树状数据：
        表的结构如下的数据即树状数据，id表示当前元素的标识，pid为当前元素的父元素标识，cost代表由当前元素到父元素的代价。

        id   | pid | cost(optional) |
        1019 | 801 |  9.2           |
        1018 | 801 |  9.1           |
        1020 | 1018|  9.0           |

    2. 计算结果：
        id   | pid | cost(optional) |
        1019 | 801 |  9.2           |
        1020 | 801 |  18.1          |
        1020 | 1018|   9.0          |

        计算结果中id列表示所有的叶子节点，pid列为叶子节点所有的祖父节点，并按照代价由上到下排列，即根节点到叶子节点的整个路径。
        至于为什么以这样的方式存储路径，是发现该结构能应付最广泛的业务。

    3. 用法：
        - 若表中只有一棵树，将元素列名和父元素列名传入，若需要计算代价，将代价列名传入
            alg = FetchAllPaths('id', 'pid', weight='cost')
            result = alg.run(df, spark)
        - 若表中有多棵树，需要将能唯一标识一棵树的所有列名传入
            alg = FetchAllPaths('id', 'pid', weight='cost', limit_cols=['col1', 'col2',...])
            result = alg.run(df, spark)

    """

    def run(self, df: DataFrame, spark: SparkSession):
        weight_name = self.weight_name

        # 添加默认的权重，用于将路径由根节点到叶子节点到从上至下排列
        if weight_name is None:
            weight_name = "weight"
            df = df.withColumn(weight_name, f.lit(1.))

        origin_cols = {*df.columns} - {self.child_col_name, self.parent_col_name, weight_name}
        origin_cols = list(map(lambda col: "direct." + col, origin_cols))

        ls = ""

        if len(self.limit_cols) != 0:
            limit_cols = map(lambda col: f"direct.{col} = indirect.{col}", self.limit_cols)
            ls = " AND " + " AND ".join(limit_cols)

        df = df.repartition(self.parallelism).persist()
        df.createOrReplaceTempView("origin")

        df_cnt = 1
        cnt = 1
        df_seed = spark.sql(f"""
            SELECT * FROM origin 
            WHERE {self.child_col_name} NOT IN
            (
                SELECT DISTINCT {self.parent_col_name}
                FROM origin
            )
        """).repartition(self.parallelism).persist()

        print("Found all leaf")

        df_seed.createOrReplaceTempView("vt_seed0")

        start_time = datetime.now()
        while df_cnt != 0:
            tblnm = "vt_seed" + str(cnt - 1)
            tblnm1 = "vt_seed" + str(cnt)
            df_seed = spark.sql(f"""
                SELECT 
                    {",".join(origin_cols) + ',' if len(origin_cols) != 0 else ""}
                    direct.{self.child_col_name},
                    indirect.{self.parent_col_name},
                    direct.{weight_name} + indirect.{weight_name} as {weight_name}
                FROM {tblnm} direct, origin indirect
                WHERE direct.{self.parent_col_name} = indirect.{self.child_col_name} {ls}
            """)
            df_seed = df_seed.repartition(self.parallelism).persist()
            df_cnt = df_seed.count()
            print(f"Layers :{cnt}, nodes: {df_cnt}, time: {datetime.now() - start_time}")
            if df_cnt != 0:
                df_seed.createOrReplaceTempView(tblnm1)
            cnt += 1

        fin_query = ""
        for a in range(cnt - 1):

            if a == 0:
                fin_query = fin_query + f"""
                    SELECT {",".join(df.columns)}
                    FROM vt_seed{a}
                """

            else:
                fin_query = fin_query + f"""
                    UNION ALL
                    SELECT {",".join(df.columns)}
                    FROM vt_seed{a}
                """

        result = spark.sql(fin_query)

        return result

    def __init__(self, child_col_name, parent_col_name, weight_name=None, limit_cols=None, parallelism=4):
        self.child_col_name = child_col_name
        self.parent_col_name = parent_col_name
        self.parallelism = parallelism
        self.weight_name = weight_name
        if limit_cols is None:
            self.limit_cols = []
        else:
            self.limit_cols = limit_cols

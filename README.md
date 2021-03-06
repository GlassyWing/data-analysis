# 基于spark的各种数据分析算法

## 基于Spark SQL的找出树状数据表中的所有路径与其相应的权重（代价）算法

1. 树状数据：
    表的结构如下的数据即树状数据，id表示当前元素的标识，pid为当前元素的父元素标识，cost代表由当前元素到父元素的代价。

    | id   | pid  | cost(optional) |
    | ---- | ---- | -------------- |
    | 1019 | 801  | 9.2            |
    | 1018 | 801  | 9.1            |
    | 1020 | 1018 | 9.0            |

2. 计算结果：

    | id   | pid  | cost(optional) |
    | ---- | ---- | -------------- |
    | 1019 | 801  | 9.2            |
    | 1020 | 801  | 18.1           |
    | 1020 | 1018 | 9.0            |

    计算结果中id列表示所有的叶子节点，pid列为叶子节点所有的祖父节点，并按照代价由上到下排列，即根节点到叶子节点的整个路径。
    至于为什么以这样的方式存储路径，是发现该结构能应付最广泛的业务。

3. 用法：
    - 若表中只有一棵树，将元素列名和父元素列名传入，若需要计算代价，将代价列名传入:

        ```python
        alg = FetchAllPaths('id', 'pid', weight='cost')
        result = alg.run(df, spark)
        ```

    - 若表中有多棵树，需要将能唯一标识一棵树的所有列名传入

        ```python
        alg = FetchAllPaths('id', 'pid', weight='cost', limit_cols=['col1', 'col2',...])
        result = alg.run(df, spark)
        ```
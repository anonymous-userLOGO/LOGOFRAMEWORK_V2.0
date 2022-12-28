from pyspark.sql import SparkSession, DataFrameReader, DataFrame
from pyspark.rdd import RDD
from py4j.java_gateway import JavaGateway, java_import, JavaObject
from pyspark import copy_func, since, _NoValue
from pyspark.serializers import ArrowStreamSerializer, BatchedSerializer, PickleSerializer, \
    UTF8Deserializer, AutoBatchedSerializer


CONTEXT_PACKAGES = [
    "org.apache.spark.sql.RspContext",
    "org.apache.spark.sql.RspDataFrameReader",
]


class RspSparkSession(SparkSession):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 在py4j调用的jvm环境下无法使用scala implicit class，需要直接生成对象 RspContext.xxFunc对象
        for package in CONTEXT_PACKAGES:
            java_import(self._jvm, package)
    
    @classmethod
    def from_spark(cls, spark):
        return cls(spark._sc, spark._jsparkSession)

    @property
    def rspRead(self):
        return RspDataFrameReader(self._wrapped)


class RspDataFrameReader(DataFrameReader): 

    def __init__(self, spark):
        self._jreader = spark._jvm.RspContext.SparkSessionFunc(spark.sparkSession._jsparkSession).rspRead()
        self._spark = spark

    def _df(self, jdf):
        return RspDataFrame(jdf, self._spark)


class RspDataFrame(DataFrame):

    def __init__(self, jdf, sql_ctx):
        super().__init__(jdf, sql_ctx)
        self._rsp_funcs = sql_ctx._jvm.RspContext.RspDatasetFunc(jdf)
        
    @property
    @since(1.3)
    def rdd(self):
        """Returns the content as an :class:`pyspark.RDD` of :class:`Row`.
        """
        if self._lazy_rdd is None:
            jrdd = self._jdf.javaToPython()
            self._lazy_rdd = RspRDD(jrdd, self.sql_ctx._sc, BatchedSerializer(PickleSerializer()), self._rsp_funcs)
        return self._lazy_rdd

    def getSubDataset(self, nums: int = 1):
        return RspDataFrame(
            self._rsp_funcs.getSubDataset(nums),
            self.sql_ctx
        )
    

class RspRDD(RDD):
    
    def __init__(self, jrdd, ctx, jrdd_deserializer=AutoBatchedSerializer(PickleSerializer()), jdf_funcs=None):    
        super().__init__(jrdd, ctx, jrdd_deserializer)
        # 这里jrdd为jvm中的JavaRDD(MapPartitionRDD) = DataFrame.javaToPython
        # 无法调用RspRDD的方法，新的参数jdf_funcs用于调用上层DataFrame.getSubDataset().javaToPython()获取目标分块的rdd。
        self._jdf_funcs = jdf_funcs

    def getSubPartitions(self, nums: int):
        sub_jdf = self._jdf_funcs.getSubDataset(nums)
        jrdd = sub_jdf.javaToPython()

        return RspRDD(
            jrdd,
            self.ctx,
            self._jrdd_deserializer,
            self.ctx._jvm.RspContext.RspDatasetFunc(sub_jdf)
        )

    
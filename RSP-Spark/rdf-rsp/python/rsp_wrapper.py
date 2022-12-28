import pyspark
from pyspark.sql import SparkSession
from pyspark import TaskContext
from pyspark.sql.dataframe import DataFrame
from pyspark import RDD
from py4j.java_gateway import JavaGateway, java_import, JavaObject
from py4j.java_collections import JavaList, ListConverter
import typing


RSP_JAVA_PACKAGE = "org.szu.rsp"


class RSPDM(object):

    JAVA_CLASS = f"{RSP_JAVA_PACKAGE}.RSPDM"

    def __init__(self, rspdmsPath: str, spark: SparkSession, jdm: JavaObject=None):
        """
        
        Args:
            rspdmsPath: str, required.
            spark: SparkSession, required, jvm should be able to import org.szu.rsp.
            jdm: org.szu.rsp.RSPDM object in jvm, optional.
        """
        self.path = rspdmsPath
        self.spark = spark
        self._jvm = self.spark._jvm
        java_import(self._jvm, RSPDM.JAVA_CLASS)
        if isinstance(jdm, JavaObject):
            self._jdm = jdm
        else:
            self._jdm = self._jvm.RSPDM(self.path, self.spark._jsparkSession)

    def convert(self, srcPath: str, rspNum: int, tarPath: str, srcType: str="", tarType: str=""):
        """Use random sample partition method to convert data from scrPath and output the result data to tarPath.

        Args:
            srcPath: str, required, path of source file.
            rspNum: int, required, number of partitions of result data.
            tarPath: str, required, path of target file.
            srcType: str, optional, type of source file, support: csv, parquet, json and txt.
            tarType: str, optional,type of target file, support: csv, parquet, json and txt.
    
        """
        if srcType and tarType:
            self._jdm.convert(srcPath, rspNum, tarPath, srcType, tarType)
        elif srcType:
            self._jdm.convert(srcPath, rspNum, tarPath, srcType)
        else:
            self._jdm.convert(srcPath, rspNum, tarPath)
    
    def getDataset(self, databaseName: str, datasetName: str, dataType: str=""):
        args = (databaseName, datasetName, dataType) if dataType else (databaseName, datasetName)
        jds = self._jdm.getDataset(*args)
        return RSPDataset(
            self.getDatasetPath(databaseName, datasetName),
            self.spark,
            dataType, 
            jds
        )

    def getDatasetPath(self, databaseName: str, datasetName: str):
        return self._jdm.getDatasetPath(databaseName, datasetName)

class RSPDataset(object):

    JAVA_CLASS = f"{RSP_JAVA_PACKAGE}.RSPDataset"

    def __init__(self, rspdmsPath: str, spark: SparkSession, srcType: str="", jds: JavaObject=None):
        """
        
        Args:
            rspdmsPath: str, required, path of hdfs file.
            spark: SparkSession, required, jvm should be able to import org.szu.rsp.
            srcType: str, optional, type of hdfs file (rspdmsPath), support: csv, parquet, json and txt.
            jds: org.szu.rsp.RSPDataset object in jvm, optional.
        """
        self.dataPath = rspdmsPath
        self.spark = spark
        self._sql_ctx = self.spark._sc 
        self.dType = srcType
        self._jvm = self.spark._jvm
        java_import(self._jvm, RSPDataset.JAVA_CLASS)
        if isinstance(jds, JavaObject):
            self._jds = jds
        else:
            if srcType:
                self._jds = self._jvm.RSPDataset(self.dataPath, self.spark._jsparkSession, srcType)
            else:
                self._jds = self._jvm.RSPDataset(self.dataPath, self.spark._jsparkSession)
        self._j_blockNames = self._jds.blockNames()
        self._j_rspBlockLocation = self._jds.rspBlockLocation()
        self.blockNames = list(self._j_blockNames)
        self.rspBlockLocation = dict(self._j_rspBlockLocation)
        self.rspBlockNumber = self._jds.rspBlockNumber()

    def takePartitions(self, partitionList: typing.Union[typing.List[str], JavaList]) -> DataFrame:
        """Read specific partitions as a DataFrame with one partition.
        
        Args:
            partitionList: typing.List[str] or JavaList, required, list of partition filenames in 
                repdmsPath, sublist of self.blockNames .

        return pyspark.sql.DataFrame
        """
        if not isinstance(partitionList, JavaList):
            partitionList = ListConverter().convert(partitionList, self._sql_ctx._gateway._gateway_client)
        jdf = self._jds.takePartitions(partitionList)
        return DataFrame(jdf, self.spark._wrapped)

    def mapF2P(self, partitionList: typing.Union[typing.List[str], JavaList], function) -> RDD: 
        """Return a new rdd by applying input function to the partition of a RDD made by 
        specific partitions in input partitionList.
        
        Args:
            partitionList: typing.List[str] or JavaList, required, list of partition filenames in 
                repdmsPath, sublist of self.blockNames .
            function: function(List[pyspark.sql.Row]) -> Anything, required

        return pyspark.RDD
        """
        return self.takePartitions(partitionList).rdd.mapPartitions(lambda iterator: [function(list(iterator))])

    def mapFs2P(self, partitionList: typing.Union[typing.List[str], JavaList], function_map: dict) -> RDD:
        """Return a new rdd by applying function switched by partitionID from function_map to the partition of a RDD made by 
        specific partitions in input partitionList.
        
        Args:
            partitionList: typing.List[str] or JavaList, required, list of partition filenames in 
                repdmsPath, sublist of self.blockNames .
            function_map: Dict[key, function(List[pyspark.sql.Row]) -> Anything], required

        return pyspark.RDD
        """
        tc = TaskContext()
        return self.takePartitions(partitionList).rdd.mapPartitions(lambda iterator: [function_map[tc.partitionId()](list(iterator))])

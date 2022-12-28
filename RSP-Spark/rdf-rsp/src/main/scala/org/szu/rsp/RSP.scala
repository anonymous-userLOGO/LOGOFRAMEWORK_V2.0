
package org.szu.rsp

import java.nio.ByteBuffer
import java.util

import collection.JavaConverters._
import scala.util.Random
import com.google.gson.{Gson, JsonArray, JsonObject, JsonParser}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class DefaultPartitioner(partitions: Int) extends Partitioner() {

  var numParitions = partitions

  override def getPartition(key: Any): Int = {
    return key.toString().toInt
  }

  override def numPartitions: Int = {
    return numParitions
  }

}


class RSPDM(rspdmsPath: String, session: SparkSession) {
  var path = rspdmsPath
  var sparkSession = session

  def convert(srcPath: String, rspNum: Int, tarPath: String): Unit = {
    var srcType = RSP.getFileType(srcPath)
    var tarType = RSP.getFileType(tarPath)
    convert(srcPath, rspNum, tarPath, srcType, tarType)
  }

  def convert(srcPath: String, rspNum: Int, tarPath: String, srcType: String): Unit = {
    convert(srcPath, rspNum, tarPath, srcType, srcType)
  }

  def convert(srcPath: String, rspNum: Int,
              tarPath: String, srcType: String, tarType: String): Unit = {
    var df = srcType match {
      case "csv" => sparkSession.read.csv(srcPath)
      case "parquet" => sparkSession.read.parquet(srcPath)
      case "json" => sparkSession.read.json(srcPath)
      case "orc" => sparkSession.read.orc(srcPath)
      case _ => sparkSession.read.text(srcPath)
    }
    var partitioner = new DefaultPartitioner(rspNum)
    var newRDD = df.rdd.map(
      row => (Random.nextInt(rspNum), row)
    ).repartitionAndSortWithinPartitions(partitioner).map(row => row._2)
    var result = sparkSession.createDataFrame(newRDD, df.schema)

    tarType match {
      case "csv" => result.write.csv(tarPath)
      case "parquet" => result.write.parquet(tarPath)
      case "json" => result.write.json(tarPath)
      case "orc" => result.write.orc(tarPath)
      case _ => result.write.text(tarPath)
    }
  }

  def getDataset(databaseName: String, datasetName: String): RSPDataset = {
    return new RSPDataset(getDatasetPath(databaseName, datasetName), sparkSession)
  }

  def getDataset(databaseName: String, datasetName: String, dataType: String): RSPDataset = {
    return new RSPDataset(getDatasetPath(databaseName, datasetName), sparkSession, dataType)
  }

  def createDataset(datasetPath: String, rspNum: Int,
                    databaseName: String, datasetName: String): Unit = {
    convert(datasetPath, rspNum, getDatasetPath(databaseName, datasetName))
  }

  def getDatasetPath(databaseName: String, datasetName: String): String = {
    return path + "/" + databaseName + "/" + datasetName
  }
}

class RSPDataset(rspdmsPath: String, session: SparkSession, srcType: String) {
  var dataPath: String = rspdmsPath
  var sparkSession: SparkSession = session
  var dType: String = srcType

  var blockNames = new util.ArrayList[String]()
  var rspBlockNumber = 0
  var rspBlockLocation = new util.HashMap[String, String]()

  var fs = FileSystem.get(new Configuration())

  def this(rspdmsPath: String, session: SparkSession) {
    this(rspdmsPath, session, RSP.getFileType(rspdmsPath))
  }

  def getMetaData(path: String): Unit = {
    var files = fs.listFiles(new Path(path), false)
    blockNames.clear()
    while(files.hasNext) {
      var status = files.next()
      var filePath = status.getPath
      var filename = filePath.getName

      if(filename.startsWith("part") & !filename.endsWith("crc")) {
        var hosts = ""
        blockNames.add(filename)
        for(location <- status.getBlockLocations) {
          for (host <- location.getHosts) {
            if (hosts.length > 0) {
              hosts = hosts + ';' + host
            } else hosts = host
          }
        }
        rspBlockLocation.put(filename, hosts)
      }
    }
    rspBlockNumber = blockNames.size


  }

  def loadMetaData(path: String): Unit = {
    var metaDataFilename = dataPath + "/" + "_rsp_metadata"
    var path = new Path(metaDataFilename)
    if(fs.exists(path)) {
      loadMetaDataFile(metaDataFilename)

    } else {
      getMetaData(dataPath)
      dumpMetaDataFile(metaDataFilename)
    }
  }

  def dumpMetaDataFile(path: String): Unit = {
    var gson = new Gson()
    var jsonMetaData = new JsonObject()
    jsonMetaData.add("blockNames", gson.toJsonTree(blockNames))
    jsonMetaData.add("rspBlockLocation", gson.toJsonTree(rspBlockLocation))
    jsonMetaData.addProperty("rspBlockNumber", rspBlockNumber)
    var stream = fs.create(new Path(path))
    stream.writeBytes(gson.toJson(jsonMetaData))
    stream.flush()
    stream.close()
  }

  def loadMetaDataFile(path: String): Unit = {
    var json = new JsonParser()
    var gson = new Gson()
    var stream = fs.open(new Path(path))
    var text = IOUtils.toString(stream)
    var jsonMetaData = json.parse(text).getAsJsonObject
    var jsonBlockLocation = jsonMetaData.get("rspBlockLocation").getAsJsonObject
    blockNames = gson.fromJson(
      jsonMetaData.get("blockNames").getAsJsonArray,
      classOf[util.ArrayList[String]]
    )
    rspBlockNumber = jsonMetaData.get("rspBlockNumber").getAsInt
    for(name <- blockNames.toArray(Array[String]()).toList) {
      rspBlockLocation.put(
        name,
        jsonBlockLocation.get(name).getAsString
      )
    }
  }

//  getMetaData(dataPath)
  loadMetaData(dataPath)

  def takePartitionList(partitionList: util.List[String]): util.ArrayList[DataFrame] = {
    var partitions = new util.ArrayList[DataFrame]()

    for(p <- partitionList.toArray(Array[String]()).toList) {
      var filePath = dataPath + "/" +  p
      partitions.add(sparkSession.read.csv(filePath).coalesce(1))

    }

    return partitions
  }

  def takePartitions(partitionList: util.List[String]): DataFrame = {
    var pathList = partitionList.toArray(Array[String]()).toList.map(p => dataPath + "/" + p)
    var df = dType match {
      case "csv" => sparkSession.read.csv(pathList: _*).coalesce(1)
      case "parquet" => sparkSession.read.parquet(pathList: _*).coalesce(1)
      case "json" => sparkSession.read.json(pathList: _*).coalesce(1)
      case "orc" => sparkSession.read.orc(pathList: _*).coalesce(1)
      case _ => sparkSession.read.text(pathList: _*).coalesce(1)
    }
    return df
  }


  def mapPartitions(func: RDD[String] => Unit): Unit = {

  }


  def mapF2P(partitionList: util.List[String], function: Array[Row] => Any): RDD[Any] = {
    var prdd = takePartitions(partitionList).rdd
    var pres = prdd.mapPartitions(
      iterator => Array(function(iterator.toArray)).iterator,
      false
    )

    return pres
  }

  def mapF2P[T](partitionList: util.List[String],
                function: T => Any,
                transferFunction: (Iterator[Row], StructType) => T
   ): RDD[Any] = {
    var df = takePartitions(partitionList)
    var schema = df.schema

    var rdd = df.rdd.mapPartitions(
      iterator => Array(function(transferFunction(iterator, schema))).iterator,
      false
    )
    return rdd
  }

  def mapFs2P(partitionList: util.List[String],
              functionMap: Map[Int, Array[Row] => Any]): RDD[Any] = {
    var tcxt = TaskContext
    var prdd = takePartitions(partitionList).rdd
    var func = functionMap.get(tcxt.getPartitionId()).get

    var pres = prdd.mapPartitions(
      iterator => Array(func(iterator.toArray)).iterator,
      false
    )
    return  pres
  }

}

object RSP {
  def getFileType(path: String): String = {
    var splitPath = path.split("\\.")
    if(splitPath.length > 0) {
      return splitPath(splitPath.length-1)
    } else {
      return ""
    }
  }
}

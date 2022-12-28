package org.apache.spark.sql

import cn.edu.szu.bigdata.RspInputFormat
import org.apache.spark.api.java.JavaRDD
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.{CoalescedRDD, PartitionCoalescer, RDD, ShuffledRDD}
import org.apache.spark.rsp.{RspRDD, SonRDD}
import org.apache.spark.{Partition, Partitioner, SparkContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.HashPartitioner

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created by longhao on 2020/11/18
 */
class DefaultPartitioner(partitions: Int) extends Partitioner(){

  var numParitions = partitions

  override def getPartition(key: Any): Int ={
    return key.toString().toInt
  }

  override def numPartitions: Int = {
    return numParitions
  }

}

object RspContext {

  implicit class SparkContextFunc(context: SparkContext) extends Serializable {
    /**
     * 读取Rsp数据，该读取方法假定路径下每个文件是一个RSP数据块，有多少个HDFS文件就会有多少个Partition
     */
    def rspTextFile(
                     path: String,
                     minPartitions: Int = context.defaultMinPartitions): RspRDD[String] = context.withScope {
      context.assertNotStopped()
      val rdd=context.hadoopFile(path, classOf[RspInputFormat], classOf[LongWritable], classOf[Text],
        minPartitions).map(pair => pair._2.toString).setName(path)
      new RspRDD[String](rdd)
    }
  }

  implicit class SparkSessionFunc(sparkSession: SparkSession) extends Serializable {

    private val rspSparkSession=new RspSparkSession(sparkSession)

    /**
     * 用法类似read，比如 spark.rspRead.csv("/xxx/xxx") , rspRead.parquet("/xxx/xxx") 等
     * @return
     */
    def rspRead: RspDataFrameReader = new RspDataFrameReader(rspSparkSession)
  }

  implicit class RspDatasetFunc(rspDataset: RspDataset[Row]) extends Serializable{

    /**
     * 参数为需要collect的rsp块数量
     * @param numsPartition
     * @return
     */
    def rspCollect(numsPartition:Int): Array[Row]= {
      rspDataset.rdd.rspCollect(numsPartition)
    }

    /**
     * 无参数，默认获取1个rsp数据块
     * @return
     */
    def rspCollect(): Array[Row]= {
      rspDataset.rdd.rspCollect(1)
    }

    /**
     * 参数为 rsp数据块的数量，返回结果为RSPDataset
     * @param nums
     * @return
     */
    def getSubDataset(nums:Int): RspDataset[Row] = {
      val newrdd=rspDataset.rdd.getSubPartitions(nums)
      new RspDataset(rspDataset.sparkSession.createDataFrame(newrdd,rspDataset.schema))
    }

    /**
     * 无参数，默认返回包含一个RSP数据块的RSPDataset
     * @return
     */
    def getSubDataset(): RspDataset[Row] = {
      getSubDataset(1)
    }


    def rspDescribe(num:Int,cols: String*): DataFrame = {
      getSubDataset(num).describe(cols: _*)
    }

    def rspDescribe(cols: String*): DataFrame = {
      getSubDataset().describe(cols: _*)
    }

    /**
     * 第一个参数为选择的分区数目，第二个参数为在各个分区上应用的函数
     * @param nums
     * @param func
     * @tparam U
     * @return
     */
    def rspMapPartitions[U: ClassTag](nums:Int,func: Iterator[Row] => Iterator[U]): RDD[U] = {
      rspDataset.rdd.rspMapPartitions(nums,func)
    }

    /**
     * 对各个分区的数据调用函数并将结果作为RDD返回，每个分区的运算产生一个值
     * @param nums 选择的分区数目
     * @param func 各个分区上应用的函数，输入参数类型：Iterator[Row]，输出参数类型 U
     * @tparam U
     * @return
     */
    def rspMapF2P[U: ClassTag](nums: Int, func: Iterator[Row] => U): RDD[U] = {
      rspMapPartitions(
        nums,
        iter => Array(func(iter)).iterator
      )
    }

    /**
     * 对各个分区的数据调用函数并将结果作为RDD返回，每个分区的运算产生一个值
     * @param nums 选择的分区数目
     * @param func 各个分区上应用的函数，输入参数类型：T，输出参数类型 U
     * @param transferFunc 转换数据类型的函数，将原始传入的数据类型由 Iterator[Row] 转为 T
     * @tparam U
     * @tparam T
     * @returnT
     */
    def rspMapF2P[U: ClassTag, T: ClassTag](nums: Int, func: T => U,
                                            transferFunc: (Iterator[Row], StructType) => T): RDD[U] = {
      var schema = rspDataset.schema
      rspMapF2P[U](
        nums,
        iter => func(transferFunc(iter, schema))
      )
    }

  }

  implicit class RspRDDFunc[T: ClassTag](rdd: RspRDD[T]) extends Serializable {
    /**
     * transform方法，返回一个包含指定块的rspRDD
     * @param nums
     * @return
     */
    def getSubPartitions(nums: Int): RspRDD[T] = {
      var newPart: Array[Partition] = new Array[Partition](nums)
      if (rdd.partitions.length >= nums) {
        for (i <- 0 to nums - 1) {
          newPart(i) = rdd.partitions(i)
        }
      }else{
        newPart=Array()
      }
      new SonRDD(rdd, newPart)
    }

    /**
     * action方法,一个rsp块中的最小值，为局部最小值
     */
    def rspMin(implicit ord: Ordering[T]): T= rdd.withScope{
      getSubPartitions(1).min()
    }

    /**
     * action方法,一个rsp块中的最大值，为局部最大值
     */
    def rspMax(implicit ord: Ordering[T]): T= rdd.withScope{
      getSubPartitions(1).max()
    }

    /**
     * transform方法，自定义的rspMapPartition,增加一个块数目的参数
     */
    def rspMapPartitions[U: ClassTag](nums:Int, f: Iterator[T] => Iterator[U],preservesPartitioning: Boolean = false): RDD[U] = {
      val prdd=getSubPartitions(nums)
      prdd.mapPartitions(f,preservesPartitioning)
    }


    /**
     * action方法,导出若干个rsp数据块
     */
    def rspCollect(nums:Int):Array[T]={
      getSubPartitions(nums).collect()
    }
  }

  implicit class NewRDDFunc[T: ClassTag](rdd: RDD[T]) extends Serializable {

    /**
     * 把rdd转换为包含指定rsp数据块的 RspRDD
     * @param nums
     * @return
     */
    def toRSP(nums:Int):RspRDD[T]={
      val partitioner = new DefaultPartitioner(nums)
      new RspRDD(rdd.map(row => (Random.nextInt(nums), row)).repartitionAndSortWithinPartitions(partitioner).map(row => row._2))
    }

    def toRSPShuffle(nums: Int, partitionCoalescer: Option[PartitionCoalescer] = Option.empty): RspRDD[T] = {
      val distributePartition = (index: Int, items: Iterator[T]) => {
        var position = new Random().nextInt(nums)
        items.map { t =>
          // Note that the hash code of the key will just be the key itself. The HashPartitioner
          // will mod it with the number of total partitions.
          position = position + 1
          (position, t)
        }
      } : Iterator[(Int, T)]
      var crdd = new CoalescedRDD(
        new ShuffledRDD[Int, T, T](
          rdd.mapPartitionsWithIndexInternal(distributePartition, isOrderSensitive = true),
          new HashPartitioner(nums)),
        nums,
        partitionCoalescer).values
      new RspRDD[T](crdd)
    }
  }


  implicit class NewJavaRDDFunc[T: ClassTag](javaRDD: JavaRDD[T]) extends Serializable{

    def getSubPartitions(nums: Int): JavaRDD[T] = {
      JavaRDD.fromRDD(
        javaRDD.rdd.asInstanceOf[RspRDD[T]].getSubPartitions(nums)
      )
    }
  }

}

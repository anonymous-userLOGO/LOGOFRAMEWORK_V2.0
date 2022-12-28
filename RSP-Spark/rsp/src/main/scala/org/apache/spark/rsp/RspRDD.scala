package org.apache.spark.rsp

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by longhao on 2020/11/18
 */
class RspRDD[T: ClassTag](prev: RDD[T]) extends RDD[T](prev){

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    prev.compute(split, context)
  }


  override protected def getPartitions: Array[Partition] = prev.partitions
}

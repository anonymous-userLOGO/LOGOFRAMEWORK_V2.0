package org.apache.spark.rsp

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by longhao on 2020/11/19
 */
class RspHadoopRDD[T: ClassTag](prev: RDD[T]) extends RspRDD[T](prev) {

}

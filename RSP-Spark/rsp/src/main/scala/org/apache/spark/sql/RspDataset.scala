package org.apache.spark.sql

import org.apache.spark.rsp.RspRDD
import org.apache.spark.sql.catalyst.plans.logical.CatalystSerde
import org.apache.spark.sql.execution.QueryExecution

import scala.reflect.ClassTag

/**
 * Created by longhao on 2020/11/26
 */
class RspDataset[T: ClassTag](prev: Dataset[T])
  extends Dataset[T](prev.sparkSession,prev.queryExecution,prev.exprEnc){

  @transient private lazy val rddQueryExecution: QueryExecution = {
    val deserialized = CatalystSerde.deserialize[T](logicalPlan)
    sparkSession.sessionState.executePlan(deserialized)
  }

  override lazy val rdd: RspRDD[T] = {
    val objectType = exprEnc.deserializer.dataType
    new RspRDD(rddQueryExecution.toRdd.mapPartitions { rows =>
      rows.map(_.get(0, objectType).asInstanceOf[T])
    })
  }
}

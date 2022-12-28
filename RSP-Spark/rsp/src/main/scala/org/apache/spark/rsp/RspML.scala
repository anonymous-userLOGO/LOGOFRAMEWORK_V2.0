
package org.apache.spark.rsp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.RspDataset
import org.apache.spark.sql.RspContext._

import smile.clustering.KMeans


object RspML {
  implicit class RspDataSetMl(rspDataset: RspDataset[Row]) extends Serializable{

    def rspKMeans(numPartitions: Int, k: Int, maxIter: Int = 100,
               tol: Double = 1E-4): RDD[KMeans] = {
      rspDataset.getSubDataset(numPartitions).map(
        r => (for { x <- 0 to r.size-1}yield r.getDouble(x)).toArray
      )(rspDataset.sparkSession.implicits.newDoubleArrayEncoder).kMeans(
        k, maxIter, tol
      )
    }

  }

  implicit class DatasetMl(dataset: Dataset[Array[Double]]) extends Serializable{
    def kMeans(k: Int, maxIter: Int = 100,
               tol: Double = 1E-4): RDD[KMeans] = {

      dataset.rdd.mapPartitions(

        iter => Array[KMeans](
          KMeans.fit(
            iter.toArray,
            k, maxIter, tol
          )
        ).iterator
      )
    }
  }
}

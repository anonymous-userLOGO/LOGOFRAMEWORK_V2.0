
package org.szu.rsp

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import smile.data.{DataFrame, SparkDataTypes, SparkRowTuple}

import scala.collection.JavaConverters._



object SmileDF {

  def rowIterator2SmileDataFrame(iterator: Iterator[Row], schema: StructType): DataFrame = {
    var smileSchema = SparkDataTypes.smileSchema(schema)

    return DataFrame.of(
      iterator.map(row => SparkRowTuple(row, smileSchema)).toList.asJava
    )
  }

}


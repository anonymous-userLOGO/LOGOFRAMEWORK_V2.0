package org.apache.spark.sql

/**
 * Created by longhao on 2020/11/27
 */
class RspDataFrameReader(rspSparkSession: RspSparkSession) extends DataFrameReader(rspSparkSession){

  override def csv(path: String): RspDataset[Row]= {
    csv(Seq(path): _*)
  }

  override def csv(paths: String*): RspDataset[Row] = {
    new RspDataset[Row](format("csv").load(paths : _*))
  }

  override def csv(csvDataset: Dataset[String]): RspDataset[Row] = {
    new RspDataset[Row](super.csv(csvDataset))
  }

  override def json(paths: String*): RspDataset[Row] = {
    new RspDataset[Row](format("json").load(paths : _*))
  }

  override def json(path: String): RspDataset[Row] = {
    json(Seq(path): _*)
  }

  override def json(jsonDataset: Dataset[String]): RspDataset[Row] = {
    new RspDataset[Row](super.json(jsonDataset))
  }

  override def parquet(path: String): RspDataset[Row] = {
    parquet(Seq(path): _*)
  }

  override def parquet(paths: String*): RspDataset[Row] = {
    new RspDataset[Row](format("parquet").load(paths: _*))
  }

  override def orc(path: String): RspDataset[Row] = {
    orc(Seq(path): _*)
  }

  override def orc(paths: String*): RspDataset[Row] = {
    new RspDataset[Row](format("orc").load(paths : _*))
  }

  override def text(path: String): RspDataset[Row] = {
    text(Seq(path): _*)
  }

  override def text(paths: String*): RspDataset[Row] = {
    new RspDataset[Row](format("text").load(paths : _*))
  }
}

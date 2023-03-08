package com.kgc.mui.impl

import com.kgc.mui.ReadHiveDim
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadHiveDimImpl extends ReadHiveDim {
  override def readHiveDataByName(spark: SparkSession, tabName: String): DataFrame = {
    spark.sql("select * from "+tabName).cache()
  }
}
object ReadHiveDimImpl{
  def apply(): ReadHiveDimImpl = new ReadHiveDimImpl()
}

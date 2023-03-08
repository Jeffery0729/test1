package com.kgc.mui

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

trait WirteMySQL {
  def write(spark:SparkSession,data:DataFrame,mysqlTab:String):Unit
}

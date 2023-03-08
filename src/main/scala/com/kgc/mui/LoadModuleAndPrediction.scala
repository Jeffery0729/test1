package com.kgc.mui

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

trait LoadModuleAndPrediction {

  def prediction(spark:SparkSession, wideTab:DataFrame, modulePath:String):DataFrame
}

package com.kgc.mui

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

trait ReadHiveDim {
    def readHiveDataByName(spark:SparkSession, tabName:String):DataFrame

}

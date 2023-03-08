package com.kgc.mui

import com.kgc.mui.impl.{LoadModuleAndPreidctionImpl, ReadKakfaImpl, WriteMySQLImpl}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UserInterestedPrediction {
  def main(args: Array[String]): Unit = {
    var spark= SparkSession.builder()
      .config("hive.metastore.uris","thrift://192.168.244.128:9083")
      .config("spark.sql.warehouse.dir","file:///e:/sparkhome")
      .enableHiveSupport()
      .master("local[*]").appName("mip").getOrCreate()
    val dm = DataMerge()
    //读kafka获取test
    ReadKakfaImpl(dm).readTestData(spark)
//    val wideTab = spark.read.option("header", "true").csv("file:///E:/abcde.csv").drop("event_hour1")
//    val prediction = LoadModuleAndPreidctionImpl().prediction(spark, wideTab, "hdfs://192.168.244.128:9000/userModule")
    //将预测的结果存放到mysql
//    WriteMySQLImpl().write(spark, prediction, "predResult")

  }
}

package com.kgc.mui.impl

import java.util.Properties

import com.kgc.mui.WirteMySQL
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

class WriteMySQLImpl extends WirteMySQL{
  override def write(spark:SparkSession, data: DataFrame, mysqlTab: String): Unit = {
    println("................預測結果寫入MySQL................")
    val prop = new Properties()

    prop.setProperty("user","root")
    prop.setProperty("password","wdq")
    val dfs = data.select("user_id", "event_id", "prediction")


    dfs.write.mode("overwrite")
      .jdbc("jdbc:mysql://192.168.244.128:3306/interesting",mysqlTab,prop)
  }
}
object WriteMySQLImpl{
  def apply(): WriteMySQLImpl = new WriteMySQLImpl()
}

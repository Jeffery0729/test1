package com.kgc.mui.impl

import com.kgc.mui.LoadModuleAndPrediction
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

class LoadModuleAndPreidctionImpl extends LoadModuleAndPrediction{
  override def prediction(spark: SparkSession, wideTab: DataFrame, modulePath: String): DataFrame = {

    println("..............加載模型進行預測...........")
    //加载模型
    val model = PipelineModel.load(modulePath)
    //將數據集中的所有的列都轉爲double型
    val colType = wideTab.columns.map(f => col(f).cast(DoubleType))
    val newWideTab = wideTab.select(colType:_*)
    //对数据进行预测
    model.transform(newWideTab.na.fill(0)).select("user_id","event_id","prediction")
  }
}
object LoadModuleAndPreidctionImpl{
  def apply(): LoadModuleAndPreidctionImpl = new LoadModuleAndPreidctionImpl()
}

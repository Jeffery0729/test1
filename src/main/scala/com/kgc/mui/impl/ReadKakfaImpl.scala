package com.kgc.mui.impl

import com.kgc.mui.{DataMerge, ReadKafka}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Duration, Durations, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ListBuffer

case class Tests(userid:String,eventid:String,invited:String,times:String)
class ReadKakfaImpl(dm:DataMerge) extends ReadKafka{

  override def readTestData(spark:SparkSession): Unit = {
    //根据用户传入的sc建造ssc
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val kafkaParam = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.244.128:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "cm005",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName
    )
    val conumser = KafkaUtils.createDirectStream(
      ssc, LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String,String](Array("test"), kafkaParam))
    import spark.implicits._
    conumser
      .mapPartitions(part=>{
            //连接
            var lb = ListBuffer[Tests]()
            for(cr:ConsumerRecord[String,String]<-part){
              val infos = cr.value().split(",",-1)
              lb.append(Tests(infos(0),infos(1),infos(2),infos(3)))
            }
            lb.iterator
        }).window(Durations.seconds(10))
      .foreachRDD(rdd=>{
        dm.fact = rdd.toDF()
        if (dm.dims.isEmpty ) {
          println("读取维度表信息 加载到内存")
          //读hive获取维度表
          dm.dims += (("customer", ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_customers")))
          dm.dims += (("eventAttendees", ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_eventAttendees")))
          dm.dims += (("eventGroups", ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_eventGroups")))
          dm.dims += (("events", ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_events")))
          dm.dims += (("locales", ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_locales")))
          dm.dims += (("userFriends", ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_userFriends")))
        }
        if(dm.fact.count()!=0) {
          //整合合并成宽表
          var wideTab = dm.dataHandler(spark)
          wideTab.coalesce(1).write.option("header",true).csv("file:///e:/abc.csv")
          //调取hdfs模型进行预测
          val prediction = LoadModuleAndPreidctionImpl().prediction(spark, wideTab, "hdfs://192.168.244.128:9000/userModule")
          //将预测的结果存放到mysql
          WriteMySQLImpl().write(spark, prediction, "predResult")
        }
      })
    ssc.start()
    ssc.awaitTermination()

  }
}
object ReadKakfaImpl{
  def apply(dm: DataMerge): ReadKakfaImpl = new ReadKakfaImpl(dm)
}

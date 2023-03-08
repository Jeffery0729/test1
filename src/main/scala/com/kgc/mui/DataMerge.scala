package com.kgc.mui


import com.kgc.mui.WideChanage.{normalfunc, samecity}
import com.kgc.mui.impl.ReadHiveDimImpl
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
class DataMerge {
    var fact:DataFrame=null
    var dims:mutable.HashMap[String,DataFrame]=mutable.HashMap[String,DataFrame]()
    val normalfunc = udf({
      (cval:Double,maxval:Double,minval:Double)=>{
        (cval-minval)/(maxval-minval)
      }
    })
    val samecity = udf({
      (addr: String, city: String, state: String, country: String) => {
        var addr1 = addr
        var city1 = city
        var state1 = state
        var country1 = country
        if (addr == null) {
          0
        }else {
          addr1 = addr.toUpperCase

          if (country1 == null) {
            country1 = "-----"
          }
          if (city == null) {
            city1 = "-----"
          }
          if (country1 == null) {
            country1 = "-----"
          }
          if (addr1.indexOf(city1.toUpperCase) != -1 || addr1.indexOf(state1.toUpperCase) != -1 || addr1.indexOf(country1.toUpperCase) != -1) {
            1
          }
          else {
            0
          }
        }
      }
    })

    def dataHandler(spark:SparkSession):DataFrame={
      import spark.implicits._

      //会议表和会议组表聚合成会议信息表
      var fact=spark.read.option("header",true).csv("D:/fileDownload/file/data/test.csv")
        .toDF("userid","eventid","invited","times")
      val dims:mutable.HashMap[String,DataFrame] =mutable.HashMap(
        "customers" -> ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_customers"),
        "eventAttendees" -> ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_eventAttendees"),
        "eventGroups" -> ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_eventGroups"),
        "events" -> ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_events"),
        "userFriends" -> ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_userFriends"),
        "locales" -> ReadHiveDimImpl().readHiveDataByName(spark, "dwd_interesting.dwd_locales")
      )

      //测试集去重：按照userid和eventid
      fact=fact.select($"userid",$"eventid",$"invited",$"times",
        row_number().over(Window.partitionBy("userid","eventid").orderBy(desc("times"))).as("rank"))
        .filter($"rank"===lit(1)).drop("rank")


      //会议表和会议组表聚合成会议信息表
      val t1 = dims("eventGroups").groupBy("grouptype")
        .agg(count("eventid").as("group_count"))
      val t2 = dims("eventGroups").join(t1, Seq("grouptype"))
      val dws_events = dims("events").join(t2, Seq("eventid")).drop("userid")


      //将用户表转为数字信息
      val mmday = dims("customers").agg(max("joinedat").as("max_day"), min("joinedat").as("min_day"))
      val dws_customers = dims("customers").join(mmday)
        .withColumn("age", year(current_date()) - $"birthyear")
        .withColumn("gender", when($"gender" === lit("male"), 0)
          .when($"gender" === lit("female"), 1)
          .otherwise(2))
        .withColumn("member_day", normalfunc($"joinedat", $"max_day", $"min_day"))
        .withColumnRenamed("localename", "locale")
        .drop("max_day", "min_day", "birthyear", "joinedat")


      //测试集用户自身情况表
      var e1=fact.join(dims("events"),Seq("eventid"),"left")
        .select($"eventid",fact("userid"),datediff($"starttime",$"times").as("invited_day"))
      var e2=dims("eventAttendees").groupBy("userid","status")
        .agg(count("eventid").as("cnt")).drop("eventid")
        .groupBy("userid")
        .agg(max(when($"status"===lit("yes"),$"cnt").otherwise(0)).as("attended_count"),
          max(when($"status"===lit("no"),$"cnt").otherwise(0)).as("not_attended_count"),
          max(when($"status"===lit("maybe"),$"cnt").otherwise(0)).as("maybe_attended_count"),
          max(when($"status"===lit("invited"),$"cnt").otherwise(0)).as("invited_event_count")
        ).drop("status")
      var e3=dims("userFriends").groupBy("userid").agg(count($"friendid").as("uf_count"))

      var dws_custinfos=fact.join(e1, Seq("userid", "eventid"), "left").join(e2, Seq("userid"), "left")
        .join(e3, Seq("userid"), "left").select(fact("userid"), fact("eventid"),
        $"invited_day", $"attended_count", $"not_attended_count",
        $"maybe_attended_count", $"invited_event_count", $"uf_count"
      ).na.fill(0)


      //测试集用户朋友的到会情况表
      val euf1=fact.join(dims("eventAttendees").as("ea"), Seq("eventid"), "inner")
        .select(fact("eventid"), fact("userid"), $"ea.userid".as("other_user"), $"ea.status")
      val euf2=euf1.join(dims("userFriends").as("uf"),euf1("userid")===$"uf.userid" && euf1("other_user")===$"uf.friendid","inner")
        .groupBy(euf1("eventid"),euf1("userid"),euf1("status"))
        .agg(count($"uf.friendid").as("cnt"))
        .groupBy("eventid","userid")
        .agg(
          max(when($"status"===lit("yes"),$"cnt").otherwise(0)).as("uf_attended_count"),
          max(when($"status"===lit("no"),$"cnt").otherwise(0)).as("uf_not_attended_count"),
          max(when($"status"===lit("maybe"),$"cnt").otherwise(0)).as("uf_maybe_attended_count"),
          max(when($"status"===lit("invited"),$"cnt").otherwise(0)).as("uf_invited_count")
        )
      val efcount=fact.join(dims("userFriends"),Seq("userid"),"left").groupBy("userid")
        .agg(count("friendid").as("fri_count"))
        .select("userid","fri_count")
      val dws_event_ufs= fact.join(euf2, Seq("userid", "eventid"), "left")
        .select("userid", "eventid", "uf_attended_count", "uf_not_attended_count", "uf_maybe_attended_count", "uf_invited_count")
        .na.fill(0).join(efcount, Seq("userid"), "left")
        .withColumn("uf_invited_prec",
          when($"fri_count" === lit(0), 0).otherwise($"uf_invited_count" / $"fri_count"))
        .withColumn("uf_attended_prec",
          when($"fri_count" === lit(0), 0).otherwise($"uf_attended_count" / $"fri_count"))
        .withColumn("uf_not_attended_prec",
          when($"fri_count" === lit(0), 0).otherwise($"uf_not_attended_count" / $"fri_count"))
        .withColumn("uf_maybe_prec",
          when($"fri_count" === lit(0), 0).otherwise($"uf_maybe_attended_count" / $"fri_count"))



      //测试集会议情况统计
      val events_info_t1 = fact.select("eventid").distinct()

      val events_info_t2 = dims("eventAttendees").join(events_info_t1.as("t1"), Seq("eventid"), "left")
        .where("t1.eventid is not null")
      val events_info_t3 = events_info_t2.groupBy("eventid", "status").agg(count("userid").as("cnt_people"))
        .select("eventid", "status", "cnt_people")
      val events_info_t4 = events_info_t3.groupBy("eventid").agg(
        max(when($"status" === lit("yes"), $"cnt_people").otherwise(0)).as("event_attended_count"),
        max(when($"status" === lit("no"), $"cnt_people").otherwise(0)).as("event_not_att_count"),
        max(when($"status" === lit("maybe"), $"cnt_people").otherwise(0)).as("event_maybe_count"),
        max(when($"status" === lit("invited"), $"cnt_people").otherwise(0)).as("event_invited_count")
      )
      val events_info_t5 = dims("events").join(events_info_t1.as("t1"), Seq("eventid"), "left").where("t1.eventid is not null")
      val events_info_t6 = events_info_t5.withColumn("event_month", month($"starttime"))
        .withColumn("event_dayofweek", date_format($"starttime", "u"))
        .withColumn("event_hour", regexp_extract($"starttime", ".*T([0-9]{2}:[0-9]{2}:[0-9]{2})\\.[0-9]{3}Z", 1))
        .select("eventid", "event_month", "event_dayofweek", "event_hour", "city", "country")

      val events_info_t7 = dims("events").groupBy("city").agg(count("eventid").as("event_cnt")).select("city", "event_cnt")
      val events_info_t8 = events_info_t7.withColumn("city_level", row_number().over(Window.partitionBy("city").orderBy(desc("event_cnt"))).as("city_level"))
      val events_info_t9 = dims("events").groupBy("country").agg(count("eventid").as("event_cnt")).select("country", "event_cnt")
      val events_info_t10 = events_info_t9.withColumn("country_level", row_number().over(Window.partitionBy("country").orderBy(desc("event_cnt"))).as("country_level"))
      val events_info_t11 = dims("events").agg(
        max($"lat".cast(DoubleType)).as("max_lat"),
        min($"lat".cast(DoubleType)).as("min_lat"),
        max($"lng".cast(DoubleType)).as("max_lng"),
        min($"lng".cast(DoubleType)).as("min_lng")
      ).select("max_lat", "min_lat", "max_lng", "min_lng")
      val events_info_t12 = events_info_t5.join(events_info_t11)
        .withColumn("lat_prec", normalfunc($"lat", $"max_lat", $"min_lat"))
        .withColumn("lng_prec", normalfunc($"lng", $"max_lng", $"min_lng"))
        .select("eventid", "lat_prec", "lng_prec")

      val dws_events_info = fact.join(events_info_t4, Seq("eventid"), "left")
        .join(events_info_t6, Seq("eventid"), "left")
        .join(events_info_t8, Seq("city"), "left")
        .join(events_info_t10, Seq("country"), "left")
        .join(events_info_t12, Seq("eventid"), "left")
        .select(fact("eventid"), fact("userid"), $"event_attended_count"
          , $"event_not_att_count", $"event_maybe_count", $"event_invited_count", $"event_month"
          , $"event_dayofweek", $"event_hour", $"city_level", $"country_level", $"lat_prec", $"lng_prec")


      //测试集用户是否是会议主持人的朋友 人与会议是否同城
      val event_oth_t1 = fact.join(dims("customers"), Seq("userid")).withColumn("user_addr", when($"location" === null, ""))
        .select(fact("userid"), fact("eventid"), $"user_addr")
      val event_oth_t2 = dims("events").as("e").join(event_oth_t1.as("t1"), Seq("eventid"), "left").where("t1.eventid != null")
        .select($"e.eventid", $"e.userid",$"e.state", $"e.city", $"e.country")
      val event_oth_t3 = dims("userFriends").as("uf")
        .join(event_oth_t2.as("t2"), Seq("userid"), "right")
        .select($"t2.*", $"uf.friendid")
      val dws_event_oth = event_oth_t1.as("t1").join(event_oth_t3.as("t3"), Seq("eventid", "userid"), "left")
        .withColumn("location_similar", samecity($"t1.user_addr", $"t3.city", $"t3.state", $"t3.country"))
        .select($"t1.eventid", $"t1.userid", when($"t3.friendid" === null, 0).otherwise(1).as("creator_is_friend")
          , $"location_similar"
        )


      //建宽表
      val result = fact.as("t")
        .join(dws_customers.as("c"), $"t.userid" === $"c.userid", "inner")
        .join(dws_custinfos.as("ci"), $"t.userid" === $"ci.userid" && $"t.eventid" === $"ci.eventid", "inner")
        .join(dws_event_ufs.as("ufs"), $"t.userid" === $"ufs.userid" && $"t.eventid" === $"ufs.eventid", "inner")
        .join(dws_events_info.as("eif"), $"t.userid" === $"eif.userid" && $"t.eventid" === $"eif.eventid", "inner")
        .join(dws_events.as("e"), $"t.eventid" === $"e.eventid", "inner")
        .join(dws_event_oth.as("eot"), $"t.eventid" === $"eot.eventid" && $"t.userid" === $"eot.userid", "inner")
        .select(
          $"t.userid".as("user_id"),
          $"t.eventid".as("event_id"),
          $"c.locale",
          $"c.gender",
          $"c.age",
          $"c.timezone",
          $"c.member_day".as("member_days"),
          $"ci.invited_day".as("invite_days"),
          $"e.group_count".as("event_count"),
          $"ci.uf_count".as("friend_count"),
          $"ci.invited_event_count",
          $"ci.attended_count",
          $"ci.not_attended_count",
          $"ci.maybe_attended_count",
          $"t.invited".as("user_invited"),
          $"ufs.uf_invited_count",
          $"ufs.uf_attended_count",
          $"ufs.uf_not_attended_count",
          $"ufs.uf_maybe_attended_count".as("uf_maybe_count"),
          $"ufs.uf_invited_prec",
          $"ufs.uf_attended_prec",
          $"ufs.uf_not_attended_prec",
          $"ufs.uf_maybe_prec",
          $"eif.event_month",
          $"eif.event_dayofweek",
          $"eif.event_hour",
          $"eif.event_invited_count",
          $"eif.event_attended_count",
          $"eif.event_not_att_count",
          $"eif.event_maybe_count",
          $"eif.city_level",
          $"eif.country_level",
          $"eif.lat_prec",
          $"eif.lng_prec",
          $"eot.creator_is_friend",
          $"eot.location_similar",
          $"e.grouptype".as("event_type")
        )
      result.show()
      result
    }
}
object DataMerge{
    def apply(): DataMerge = new DataMerge()
}

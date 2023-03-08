package com.kgc.mui


import org.apache.spark.sql.{DataFrame, SparkSession}

trait ReadKafka {
    def readTestData(spark:SparkSession):Unit
}

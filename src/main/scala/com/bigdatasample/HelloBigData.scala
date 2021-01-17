package com.bigdatasample

import org.apache.spark.sql.SparkSession

object HelloBigData {
  def main(args: Array[String]) {
    val logFile = getClass.getResource("/Avengers_Endgame.pdf").getFile // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()

    val logData = spark.read.textFile(logFile).cache()
    val tony = logData.filter(line => line.contains("avenger")).count()
    val steve = logData.filter(line => line.contains("thanos")).count()
    println(s"Lines with avenger: $tony, Lines with thanos: $steve")
    spark.stop()
  }

}

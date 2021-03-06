package com.filmforyou

import java.util.Calendar

import com.filmforyou.HelloBigData.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object FilmYearCount {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

//    val logFile = getClass.getResource("resources/filmsData.csv").getFile // Should be some file on your system
    val sc = new SparkContext("local[*]", "Film Year Counter")

    val dataHolder = sc.textFile("data/filmsData.csv")

    val startTime = Calendar.getInstance().getTimeInMillis

    println("start Time:"+startTime)

    val filmYearRDD = dataHolder.map(x => x.split(",")(0))

    val yearResult = filmYearRDD.countByValue().toSeq.sortBy(_._1)



    yearResult.map(println)

    val endTime = Calendar.getInstance().getTimeInMillis

    println("end Time:"+ endTime)

    println(s"TIME TOOKS ${(endTime- startTime).floatValue/1000} seconds")

  }

}

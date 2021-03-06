package com.filmforyou

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

package object service {


  def userProductMatrixALS(spark: SparkSession, df1: DataFrame, userId: String, moviesRatings: Map[String, String]):Map[String, String]={

    val rank = 4
    val numIterations = 15
    val lambda = 0.001
    val alpha = 1.00
    val block = -1
    val seed = 12345L
    val implicitPrefs = true
    val ratingsDF = df1.select(df1.col("user_id"),
      df1.col("movie_id"),
      df1.col("rating"))

    val splits = ratingsDF.randomSplit(Array(0.75, 0.25), seed = 12345L)
    val (trainingData, testData) = (splits(0), splits(1))

    val givenRating= spark.sparkContext.parallelize(moviesRatings.toSeq)
      .map{case (filmId, rating)=> Rating(userId.toInt, filmId.toInt, rating.toDouble)}

    val ratingsRDD = trainingData.rdd.map(row => {
      val userId = row.getString(0).trim
      val movieId = row.getString(1).trim
      val ratings = row.getString(2).trim
      Rating(userId.toInt, movieId.toInt, ratings.toDouble)
    }).union(givenRating) //++ RDD(moviesRatings.map(x => Rating(userId.toInt, x._1.toInt, x._2.toDouble)))


    val testingRDD = testData.rdd.map(row => {
      val userId = row.getString(0).trim
      val movieId = row.getString(1).trim
      val ratings = row.getString(2).trim
      Rating(userId.toInt, movieId.toInt, ratings.toDouble)
    }).union(givenRating)


    //  println("HYPER PARAMETERS ARE : "+ getHyperParams(ratingsRDD, testingRDD))

    val model = new ALS().setIterations(numIterations) .setBlocks(block).setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank)
      .setSeed(seed)
      .setImplicitPrefs(implicitPrefs)
      .run(ratingsRDD)

    println("Rating:(UserID, MovieID, Rating)")

    println("----------------------------------")

    val topRecsForUser = model.recommendProducts(userId.toInt, 30)
    for (rating <- topRecsForUser)
    {
      println(rating.toString())
    }

    topRecsForUser.toList.map(x=> x.product.toString -> x.rating.toString).toMap
  }

}

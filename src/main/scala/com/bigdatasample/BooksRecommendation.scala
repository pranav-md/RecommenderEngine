package com.bigdatasample

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BooksRecommendation extends App{

  val ratigsFile = "data/goodreads_ratings.csv"
  val spark = SparkSession.builder.appName("Book recommendation engine").master("local[*]").getOrCreate()

  val df1 = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratigsFile)

  val ratingsDF = df1.select(df1.col("user_id"),
    df1.col("book_id"),
    df1.col("rating"))
  //val hi= ratingsDF.show(false)

  //
  //  val booksFile = "data/goodreads_books.csv"
  //
  //  val df2 = spark.read.format("com.databricks.spark.csv").option("header", "true").load(booksFile)
  //
  //  val booksDF = df2.select(df2.col("book_id"),
  //                            df2.col("original_title"),
  //                            df2.col("title"))
  //
  //
  //
  //  ratingsDF.createOrReplaceTempView("ratings")
  //
  //  booksDF.createOrReplaceTempView("books")


  //  val numRatings = ratingsDF.count()
  //
  //  val numUsers = ratingsDF.select(ratingsDF.col("user_id")).distinct().count()
  //
  //  val numBooks = ratingsDF.select(ratingsDF.col("movie_id")).distinct().count()

  userProductMatrixALS

  def booksNumUsersRated():Unit={

    val results = spark.sql("select books.title, bookrates.maxr, bookrates.minr, bookrates.cntu "

      + "from(SELECT ratings.book_id ,max(ratings.rating) as maxr,"

      + "min(ratings.rating) as minr,count(distinct user_id) as cntu "

      + "FROM ratings group by ratings.book_id) bookrates "

      + "join books on bookrates.book_id =books.book_id "

      + "order by bookrates.cntu desc")

    println("RESULTS:  "+ results.show(true))
  }

  def userProductMatrixALS:Unit={

    val rank = 4
    val numIterations = 15
    val lambda = 0.001
    val alpha = 1.00
    val block = -1
    val seed = 12345L
    val implicitPrefs = true
    val ratingsDF = df1.select(df1.col("user_id"),
      df1.col("book_id"),
      df1.col("rating"))

    val splits = ratingsDF.randomSplit(Array(0.75, 0.25), seed = 12345L)
    val (trainingData, testData) = (splits(0), splits(1))

    val ratingsRDD = trainingData.rdd.map(row => {
      val userId = row.getString(0).trim
      val bookId = row.getString(1).trim
      val ratings = row.getString(2).trim
      Rating(userId.toInt, bookId.toInt, ratings.toDouble)
    })

    val testingRDD = testData.rdd.map(row => {
      val userId = row.getString(0).trim
      val bookId = row.getString(1).trim
      val ratings = row.getString(2).trim
      Rating(userId.toInt, bookId.toInt, ratings.toDouble)
    })


    //  println("HYPER PARAMETERS ARE : "+ getHyperParams(ratingsRDD, testingRDD))

    val model = new ALS().setIterations(numIterations) .setBlocks(block).setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank)
      .setSeed(seed)
      .setImplicitPrefs(implicitPrefs)
      .run(ratingsRDD)

    println("Rating:(UserID, BookID, Rating)")

    println("----------------------------------")

    val topRecsForUser = model.recommendProducts(60000, 10)
    for (rating <- topRecsForUser)
    {
      println(rating.toString())
    }

    println("----------------------------------")
  }

  // def hyperParameterTuning()

  def getHyperParams(ratingsRDD: RDD[Rating], testRDD: RDD[Rating]): (Int, Double)={
    val numIterations = 15
    val alpha = 1.00
    val block = -1
    val seed = 12345L
    val implicitPrefs = false

    val ranks = Array.range(1,11)
    val lambdas= List(0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.50, 0.55, 0.60)

    var bestRank: Int =1
    var bestLambda: Double = 0.001
    var leastError:Double = 1.0

    println("LOOP BEGINS")
    ranks.map { rank =>
      lambdas.map { lambda =>
        val model = new ALS().setIterations(numIterations).setBlocks(block).setAlpha(alpha)
          .setLambda(lambda)
          .setRank(rank)
          .setSeed(seed)
          .setImplicitPrefs(implicitPrefs)
          .run(ratingsRDD)

        val curError = computeRmse(model, testRDD, true)
        println("CUR ERROR :" + curError)
        println("CURRENT RANK :" + rank)
        println("BEST RANK :" + bestRank)
        println("LEAST ERROR :" + leastError)
        if (curError < leastError) {
          leastError = curError
          bestRank = rank
        }
      }
    }
    (bestRank, bestLambda)
  }

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x => ((x.user, x.product), x.rating) }.join(data.map(x => ((x.user, x.product), x.rating))).values
    if (implicitPrefs) {
      println("(Prediction, Rating)")
      println(predictionsAndRatings.take(5).mkString("n"))
    }
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }
}

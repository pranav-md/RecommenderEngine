package com.bigdatasample

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import scala.Tuple2
import org.apache.spark.rdd.RDD

object BookReccomendation extends App {

  val ratigsFile = "data/goodreads_ratings.csv"
  val spark = SparkSession.builder.appName("Book recommendation engine").master("local[*]").getOrCreate()

  val df1 = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratigsFile)

  val ratingsDF = df1.select(df1.col("user_id"),
                             df1.col("book_id"),
                             df1.col("rating"))
  val hi= ratingsDF.show(false)


  val booksFile = "data/goodreads_books.csv"

  val df2 = spark.read.format("com.databricks.spark.csv").option("header", "true").load(booksFile)

  val booksDF = df2.select(df2.col("book_id"),
                            df2.col("original_title"),
                            df2.col("title"))



  ratingsDF.createOrReplaceTempView("ratings")

  booksDF.createOrReplaceTempView("books")


  val numRatings = ratingsDF.count()

  val numUsers = ratingsDF.select(ratingsDF.col("user_id")).distinct().count()

  val numBooks = ratingsDF.select(ratingsDF.col("book_id")).distinct().count()

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

    val rank = 20
    val numIterations = 15
    val lambda = 0.10
    val alpha = 1.00
    val block = -1
    val seed = 12345L
    val implicitPrefs = false
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

    val model = new ALS().setIterations(numIterations) .setBlocks(block).setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank) .setSeed(seed)
      .setImplicitPrefs(implicitPrefs)
      .run(ratingsRDD)

    println("Rating:(UserID, MovieID, Rating)")

    println("----------------------------------")

    val topRecsForUser = model.recommendProducts(60000, 6)
    for (rating <- topRecsForUser)
    {
      println(rating.toString())
    }

    println("----------------------------------")


  }
}



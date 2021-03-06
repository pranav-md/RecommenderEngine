package com.filmforyou.service

import java.util.UUID

import scala.concurrent.Future
import com.filmforyou.domain.Domain.Film
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.filmforyou.scrapper.MovieDBScrapperImpl

class FilmServiceImpl(spark: SparkSession, filmsRatingsDF: DataFrame, filmsLinksDF: DataFrame, filmsDF: DataFrame) extends FilmService{

  override def getFilm(id: String): Film = {

    val filmsFile = "data/films.csv"

    val filmLinksFile = "data/links.csv"

  //  val spark = SparkSession.builder.appName("Film recommendation engine").master("local[*]").getOrCreate()

//    val filmsDataFrame = spark.read.format("com.databricks.spark.csv").option("header", true).load(filmsFile)
//
//    val filmLinksDataFrame = spark.read.format("com.databricks.spark.csv").option("header", true).load(filmLinksFile)

//    val filmsLinksDF = filmLinksDataFrame.select(filmLinksDataFrame.col("movieId"),
//      filmLinksDataFrame.col("imdbId"),
//      filmLinksDataFrame.col("tmdbId"))
//
//
//    val filmsDF = filmsDataFrame.select(filmsDataFrame.col("movieId"),
//      filmsDataFrame.col("title"),
//      filmsDataFrame.col("genres"))

//    filmsDF.createTempView("Films")
//    filmsLinksDF.createTempView("FilmLinks")

    val reqdFilmDF = spark.sql(s"select * from Films where movieId ==$id")

    val reqdFilmLinkDF = spark.sql(s"select * from FilmLinks where movieId ==$id")


    println("MOVIE imdbId= "+reqdFilmLinkDF.head().getAs[String]("imdbId"))

    val movieDBScrapper = new MovieDBScrapperImpl
    val movieData=  movieDBScrapper.getMoviePage(reqdFilmLinkDF.head().getAs[String]("imdbId"))


   val film= Film(reqdFilmDF.head().getAs[String]("title"),
         reqdFilmDF.head().getAs[String]("title")
           .substring(reqdFilmDF.head().getAs[String]("title").size-5,
                      reqdFilmDF.head().getAs[String]("title").size-1).toInt,
      reqdFilmDF.head().getAs[String]("genres").split("([|])").toList,
      movieDBScrapper.getMovieDirector(movieData), "", movieDBScrapper.getMovieImageURL(movieData),
      movieDBScrapper.getMovieRunTime(movieData), Some(movieDBScrapper.getMovieRating(movieData)))
    println("FILM= "+film)
    film
  }

  def getFilmNamesAsSubstring(filmName: String): Map[String, String]={
//    val filmsFile = "data/films.csv"
//
//    val spark = SparkSession.builder.appName("Film recommendation engine").master("local[*]").getOrCreate()
//
//    val filmsDataFrame = spark.read.format("com.databricks.spark.csv").option("header", true).load(filmsFile)
//
//
//    val filmsDF = filmsDataFrame.select(filmsDataFrame.col("movieId"),
//      filmsDataFrame.col("title"),
//      filmsDataFrame.col("genres"))
//
//    filmsDF.createTempView("Films")

    val reqdFilmsDF = spark.sql(s"select * from Films where position(lower('$filmName'), lower(title)) != 0")

    println("RESULT=" + reqdFilmsDF.collect().mkString(" ")+" FILMNAME= "+filmName)

   // val list= reqdFilmsDF.map(x=> x.getAs[String]("movieId") -> x.getAs[String]("title"))

    reqdFilmsDF.collect().map(x=> x.getAs[String]("movieId") -> x.getAs[String]("title")).toMap
  }

  override def getPredictedFilms(spark: SparkSession, moviesRatings: Map[String, String], userId: String): List[Film] = {

//    val filmRatingsFile = "data/films_ratings_promax.csv"
//
//
//    val spark = SparkSession.builder.appName("Film recommendation engine").master("local[*]").getOrCreate()
//
//    val filmRatingsDataFrame = spark.read.format("com.databricks.spark.csv").option("header", true).load(filmRatingsFile)
//
//    val filmsRatingsDF = filmRatingsDataFrame.select(filmRatingsDataFrame.col("user_id"),
//      filmRatingsDataFrame.col("movie_id"),
//      filmRatingsDataFrame.col("rating"))


  //  filmsDF.createTempView("FilmRatings")
 //   filmsRatingsDF.createTempView("FilmRatings")

    val predictedFilmsAndRatings = userProductMatrixALS(spark, filmsRatingsDF, userId, moviesRatings)

    predictedFilmsAndRatings.map(x=> getFilm(x._1).copy(userRating = Some(x._2))).toList

  }
}

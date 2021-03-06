package com.filmforyou.rest

import akka.actor.ActorSystem

import scala.util.{Failure, Success}
import akka.event.Logging
import akka.event.jul.Logger
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{PathMatchers, Route}
import akka.http.scaladsl.server.directives.DebuggingDirectives
import akka.stream.ActorMaterializer
import com.filmforyou.domain.Domain.FilmRequest
import com.filmforyou.service.FilmServiceImpl
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.circe.syntax._
import akka.http.scaladsl.server.Directives.as

import com.filmforyou.rest.FilmImplicits.{filmDecoder, filmEncoder}
import com.filmforyou.rest.FilmsRequestImplicits._
import com.filmforyou.rest.AkkaCirceSupport._

object FilmRoutes extends App {

  implicit val system = ActorSystem("my-system")
  implicit val materializer = ActorMaterializer()
  val logger = Logger(classOf[App], "")

  implicit val executionContext = system.dispatcher

  val V1_PATH = "v1"
  val FILM_ROUTE= "film"
  val spark = SparkSession.builder.appName("Film recommendation engine").master("local[*]").getOrCreate()
  val dataFrames = buildModel
  var filmService= new FilmServiceImpl(spark, dataFrames._1, dataFrames._2, dataFrames._3)

  var getFilm =
    path(PathMatchers.Segment) { filmId =>
      get {
        try {
          println("INSIDE TRY")

          val res = filmService.getFilm(filmId).asJson
          println("RESULT= "+res)
          complete(HttpEntity(ContentTypes.`application/json`, res.toString))
        }
        catch {
          case x: Exception => complete(StatusCodes.NotFound)
        }
      }
    }

  var predictFilm: Route =
    path("predict-film") {
      post
      {
          entity(as[FilmRequest]) { filmIds =>
          try {
            println("INSIDE post " + filmIds)

            val res = filmService.getPredictedFilms(spark, filmIds.films, "283250").asJson
            complete(HttpEntity(ContentTypes.`application/json`, res.toString))
          }
          catch {
            case x: Exception => complete(StatusCodes.NotFound)
          }
        }
      }
    }


  var searchFilm =
        path(PathMatchers.Segment) { filmName =>
          println("INSIDE path" + filmName)
          post {
          println("INSIDE post" + filmName)
          try {
            val res = filmService.getFilmNamesAsSubstring(filmName).asJson
            complete(HttpEntity(ContentTypes.`application/json`, res.toString))
          }
          catch {
            case x: Exception => complete(StatusCodes.NotFound)
          }
        }
      }

  val routes: Route = DebuggingDirectives.logRequest("films", Logging.InfoLevel) {
    pathPrefix(V1_PATH / FILM_ROUTE) { getFilm ~ searchFilm  } ~{ predictFilm }
  }

  val bindingFuture = Http().bindAndHandle(routes, "localhost", 8081)


  bindingFuture onComplete {
    case Success(answer) =>
      logger.info(s"Server online at http://localhost:8081/\n")

    case Failure(msg) =>
      logger.info(s"Service failed: $msg, exiting")
      System.exit(1)
  }
//  bindingFuture
//    .flatMap(_.unbind()) // trigger unbinding from the port
//    .onComplete(_ => system.terminate()) // and shutdown when done


  def buildModel: (DataFrame, DataFrame, DataFrame)={
    val ratingsProMaxFile = "data/films_ratings.csv"

    val df1 = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratingsProMaxFile)

    val filmsRatingsDF = df1.select(df1.col("user_id"),
      df1.col("movie_id"),
      df1.col("rating"))

    filmsRatingsDF.createTempView("FilmRatings")
    ////

    val filmLinksFile = "data/links.csv"

    val filmLinksDataFrame = spark.read.format("com.databricks.spark.csv").option("header", true).load(filmLinksFile)

    val filmsLinksDF = filmLinksDataFrame.select(filmLinksDataFrame.col("movieId"),
      filmLinksDataFrame.col("imdbId"),
      filmLinksDataFrame.col("tmdbId"))

    filmsLinksDF.createTempView("FilmLinks")
    ////

    val filmsFile = "data/films.csv"

    val filmsDataFrame = spark.read.format("com.databricks.spark.csv").option("header", true).load(filmsFile)

    val filmsDF = filmsDataFrame.select(filmsDataFrame.col("movieId"),
      filmsDataFrame.col("title"),
      filmsDataFrame.col("genres"))

    filmsDF.createTempView("Films")
    /////
    (filmsRatingsDF, filmsLinksDF, filmsDF)
  }
}

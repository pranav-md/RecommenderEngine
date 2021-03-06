package com.filmforyou.service

import com.filmforyou.domain.Domain.Film
import org.apache.spark.sql.SparkSession

trait FilmService {

  def getFilm(id : String): Film

  def getFilmNamesAsSubstring(filmName: String): Map[String, String]

  def getPredictedFilms(spark: SparkSession, moviesRatings: Map[String, String], userId: String): List[Film]
}

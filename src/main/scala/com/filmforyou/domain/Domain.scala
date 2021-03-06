package com.filmforyou.domain


object Domain {

  case class Film(name: String, year: Int, generes: List[String], director: String, production: String,
                  imageURL: String, totalRuntime: String, userRating: Option[String]= None)

  case class FilmRequest(films: Map[String, String])
}

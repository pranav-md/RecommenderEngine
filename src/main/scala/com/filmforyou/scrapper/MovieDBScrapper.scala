package com.filmforyou.scrapper

trait MovieDBScrapper {

  def getMovieImageURL(body: String): String

  def getMovieRunTime(body: String): String

  def getMovieDirector(body: String): String

  def getMovieRating(body: String): String

  def getMoviePage(linkId: String): String
}

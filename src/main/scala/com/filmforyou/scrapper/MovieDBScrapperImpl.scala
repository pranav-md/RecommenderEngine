package com.filmforyou.scrapper

import org.jsoup.Jsoup

class MovieDBScrapperImpl extends MovieDBScrapper{

  val filmNotFoundExp= "film_not_found"



  def getMovieImageURL(body: String): String ={
    val movieSrcTag= "\"image\": \""
    val startIndex = body.indexOf(movieSrcTag) + movieSrcTag.size
    val endIndex = body.indexOf("\",", startIndex)
    body.substring(startIndex, endIndex).mkString
  }

  def getMovieRunTime(body: String): String= {
    val runTimeTag= "<time datetime=\""
    val tagStartIndex = body.indexOf(runTimeTag)
    val startIndex = body.indexOf(">", tagStartIndex) + 1

    val endIndex = body.indexOf("</time>", startIndex)

    body.substring(startIndex, endIndex).trim
  }

  def getMovieDirector(body: String): String={
 //   println("BODY= "+body)

    val startIndex = body.indexOf("Directed by")+ "Directed by".size
    val endIndex = body.indexOf(".", startIndex)

    body.substring(startIndex, endIndex).trim
  }

  def getMovieRating(body: String): String={
 //   println("BODY= "+body)

    parseTag(body, "<span itemprop=\"ratingValue\">", "</span>").trim
  }

  def getMoviePage(linkId: String): String ={
    try {
      println(s"Link= https://www.imdb.com/title/tt${linkId}/")
      val movieDBConnection= Jsoup.connect(s"https://www.imdb.com/title/tt${linkId}/")
      val movieDBResponse = movieDBConnection.execute()
      movieDBResponse.body()
    }
    catch{
      case x:Exception => println("Scrapping failed because: "+x.getMessage)
                          filmNotFoundExp
    }
  }

}

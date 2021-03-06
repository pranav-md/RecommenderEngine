package com.filmforyou

package object scrapper {

  val posterStartTag= "<div class=\"image_content backdrop\">"
  val posterEndTag= "</div>"

  val theMovieDBInitialUrl= "https://www.themoviedb.org"


  def parseTag(body: String, startTag: String, endTag: String): String={

    val startIndex= body.indexOf(startTag) + startTag.size
    val endIndex = body.indexOf(endTag, startIndex)

    body.substring(startIndex, endIndex).trim
  }


}

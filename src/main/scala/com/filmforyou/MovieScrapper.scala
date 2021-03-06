package com.filmforyou

import org.jsoup.{Connection, Jsoup}
import scrapper.{MovieDBScrapper, MovieDBScrapperImpl}
import com.filmforyou.service.FilmServiceImpl
import java.util.Map

object MovieScrapper extends App {


 val loginForm1 = Jsoup.connect("https://www.imdb.com/title/tt0112453/").execute()
 //val loginForm1 = Jsoup.connect("https://www.themoviedb.org/movie/")
//   .userAgent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0")
//   .cookie("tmdb.prefs", "{\"adult\":false,\"i18n_fallback_language\":\"en-US\",\"locale\":\"en-US\",\"country_code\":\"IN\",\"timezone\":\"Asia/Kolkata\"}")
//   .cookie("tmdb.session", "AYeUezhERaTQcL_qeOTtg_Ykxnhlb44XIii89BgCMhbZsOEq7byBHSoUjrgA4wJ_7Zazzu7wAofOLEc4KdCwj-u6Q078KheX6eJfLN_TwEqEV7j62-4-fWz5tA0DXD_5av6CWfxXOUUwPkbMAA6pzwkZgaW6LtMcptWGMoxv0ZNtjqyj6dEAf9GCNhcn68dEDWnY7F95zpNBn-Q-dguxlpwEx_uHpBTUasGrL_625xBh2fwQ8aFpayKIAUjRtuDasQQM7r7DALy-YDl3f2Z3Isg=")
//   .execute()
//  println("IMDB SIZE="+loginForm.body().size)
//  println("themoviedb ="+loginForm1.body())
 val movieDB = new MovieDBScrapperImpl
 val URL= movieDB.getMovieImageURL(loginForm1.body())

// val film= new FilmServiceImpl
// println("FILM = " + film.getFilmNamesAsSubstring("Doctor"))

 println("BODY= "+loginForm1.body())
// println("RUNTIME BODY= "+movieDB.getMovieRunTime(loginForm1.body()))
// println("RUNTIME DIRECTOR= "+movieDB.getMovieDirector(loginForm1.body()))


 //  val data = Jsoup.connect("https://movielens.org/movies/1")
//   .data("cookieexists", "false")
//    .data("userName", "Pmd1234")
//    .data("password", "ImIronman123!")
//    .data("uvts", "c6b3eb86-6ceb-451f-7fa9-47ac62c21099")
//    .cookies(loginForm.cookies())
//    .post()
//
//  println("Data= "+ data.body)

}

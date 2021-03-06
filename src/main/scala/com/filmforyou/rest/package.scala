package com.filmforyou

import akka.http.scaladsl.common.{CsvEntityStreamingSupport, EntityStreamingSupport}
import akka.http.scaladsl.marshalling.{Marshaller, Marshalling, ToResponseMarshallable, ToResponseMarshaller}
import akka.http.scaladsl.model.{ContentTypeRange, ContentTypes, HttpEntity}
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.util.ByteString
import com.filmforyou.domain.Domain.{Film, FilmRequest}
import io.circe.{Decoder, Encoder, ObjectEncoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.decode
import io.circe.syntax._

import scala.concurrent.Future

package object rest {


  object FilmImplicits {

    implicit val filmDecoder: Decoder[Film] = deriveDecoder[Film]
    implicit val filmEncoder: Encoder.AsObject[Film]  = Encoder.forProduct8("name",
      "year", "generes","director",
        "production", "imageURL", "totalRuntime", "userRating")(x=>
      (x.name, x.year, x.generes, x.director, x.production, x.imageURL, x.totalRuntime, x.userRating))

  }
  object FilmsRequestImplicits {

    implicit val filmRequestDecoder: Decoder[FilmRequest] = deriveDecoder[FilmRequest]
 //   implicit val filmRequestEncoder: ObjectEncoder[FilmRequest] = deriveEncoder[FilmRequest]
  }
  object AkkaCirceSupport {

    private[this] def jsonContentTypes: List[ContentTypeRange] = List(`application/json`)

    implicit final def unmarshaller[E: Decoder]: FromEntityUnmarshaller[E] = {
      Unmarshaller.stringUnmarshaller
        .forContentTypes(jsonContentTypes: _*)
        .flatMap { context => materialiser => json =>
          decode[E](json).fold(Future.failed, Future.successful)
        }
    }

    implicit final def marshaller[E: Encoder]: ToResponseMarshaller[E] = {
      Marshaller.withFixedContentType(`application/json`) { entity =>
        import akka.http.scaladsl.model.HttpResponse
        HttpResponse.apply(entity = HttpEntity(`application/json`, entity.asJson.noSpaces))
      }
    }

    implicit def toResponseMarshallable[E: Encoder](input: E): ToResponseMarshallable = ToResponseMarshallable(input)

    implicit val csvStreaming: CsvEntityStreamingSupport = EntityStreamingSupport.csv()

    implicit val asCsv: Marshaller[List[String], ByteString] = Marshaller.strict[List[String], ByteString] { values =>
      Marshalling.WithFixedContentType(ContentTypes.`text/csv(UTF-8)`, () => {
        ByteString(values.mkString(","))
      })
    }
  }
}

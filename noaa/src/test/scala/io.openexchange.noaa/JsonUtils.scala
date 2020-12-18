package io.openexchange.noaa

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object JsonUtils {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def fromJson[T](json : String)(implicit m : Manifest[T]) : T = {
     mapper.readValue[T](json)
  }
}

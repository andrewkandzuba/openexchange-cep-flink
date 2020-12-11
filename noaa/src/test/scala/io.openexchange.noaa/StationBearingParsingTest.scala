package io.openexchange.noaa

import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.parsing.json.JSON

class StationBearingParsingTest  extends AnyFlatSpec with MockFactory with Matchers{

  private val jsonRaw =
    """
      |{
      |  "station": "72259513911",
      |  "observationDate": "2020-01-01T04:52:00",
      |  "reportType": "FM-15",
      |  "observationSource": "7",
      |  "hourlyDewPointTemperature": "28",
      |  "hourlyDryBulbTemperature": "57",
      |  "hourlyPrecipitation": "0.00",
      |  "hourlyRelativeHumidity": "33",
      |  "hourlySeaLevelPressure": "29.97",
      |  "hourlySkyConditions": "CLR:00",
      |  "hourlyStationPressure": "29.32",
      |  "hourlyVisibility": "",
      |  "hourlyWetBulbTemperature": "44",
      |  "hourlyWindDirection": "170",
      |  "hourlyWindGustSpeed": "",
      |  "hourlyWindSpeed": "5"
      |}
    """.stripMargin

  "Test" should "be equal" in {
    var t = JSON.parseRaw(jsonRaw)
    var i = 10
  }

}

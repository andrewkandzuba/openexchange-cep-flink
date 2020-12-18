package io.openexchange.noaa

import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDateTime

class StationBearingParsingTest  extends AnyFlatSpec with MockFactory with Matchers{

  private val stationBearing = StationBearing(
    "72259513911",
    LocalDateTime.parse("2020-06-01T00:52:00"),
    "FM-15",
    "7",
    26,
    59,
    0.00,
    28,
    30.04,
    "CLR:00",
    29.40,
    0,
    45,
    170,
    0,
    7
  )

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

  "Parsed t" should "contains" in {
    val t = JsonUtils.fromJson[StationBearing](jsonRaw)
    assert("72259513911".eq(t.station))
  }

  "JSON s" should "be equal to" in {
    val s = JsonUtils.toJson(stationBearing)
    assert("".eq(s))
  }
}

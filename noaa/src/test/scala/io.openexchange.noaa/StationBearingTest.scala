package io.openexchange.noaa

import java.time.LocalDateTime

import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class StationBearingTest extends AnyFlatSpec with MockFactory with Matchers {

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

  "A StationBearing" should "have all fields set" in {
    assert("72259513911".eq(stationBearing.station))
    assert("2020-06-01T00:52".equalsIgnoreCase(stationBearing.observationDate.toString))
    assert("FM-15".eq(stationBearing.reportType))
    assert("7".eq(stationBearing.observationSource))
    assert(26 == stationBearing.hourlyDewPointTemperature)
    assert(59 == stationBearing.hourlyDryBulbTemperature)
    assert(0.0 == stationBearing.hourlyPrecipitation)
    assert(28 == stationBearing.hourlyRelativeHumidity)
    assert(30.04 == stationBearing.hourlySeaLevelPressure)
    assert("CLR:00".eq(stationBearing.hourlySkyConditions))
    assert(29.40 == stationBearing.hourlyStationPressure)
    assert(0 == stationBearing.hourlyVisibility)
    assert(45 == stationBearing.hourlyWetBulbTemperature)
    assert(170 == stationBearing.hourlyWindDirection)
    assert(0 == stationBearing.hourlyWindGustSpeed)
    assert(7 == stationBearing.hourlyWindSpeed)
  }

  StationBearing.unapply(stationBearing).get should be((
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
  ))
}

package io.openexchange.noaa

import java.time.LocalDate

case class StationBearing(station: String,
                          date: LocalDate,
                          reportType: String,
                          source: String,
                          hourlyAltimeterSetting: String,
                          hourlyDewPointTemperature: String,
                          hourlyDryBulbTemperature: String,
                          hourlyPrecipitation: String,
                          hourlyRelativeHumidity: Double,
                          hourlySeaLevelPressure: Int,
                          hourlySkyConditions: String,
                          hourlyStationPressure: Double,
                          hourlyVisibility: Double,
                          hourlyWetBulbTemperature: Int,
                          hourlyWindDirection: Int,
                          hourlyWindGustSpeed: Int,
                          hourlyWindSpeed: Int)

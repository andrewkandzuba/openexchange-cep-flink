package io.openexchange.noaa

import java.time.LocalDateTime

final case class StationBearing(station: String,
                          observationDate: LocalDateTime,
                          reportType: String,
                          observationSource: String,
                          hourlyDewPointTemperature: Int,
                          hourlyDryBulbTemperature: Int,
                          hourlyPrecipitation: Double,
                          hourlyRelativeHumidity: Double,
                          hourlySeaLevelPressure: Double,
                          hourlySkyConditions: String,
                          hourlyStationPressure: Double,
                          hourlyVisibility: Double,
                          hourlyWetBulbTemperature: Int,
                          hourlyWindDirection: Int,
                          hourlyWindGustSpeed: Int,
                          hourlyWindSpeed: Int)
package io.openexchage.aws.s3

import java.nio.charset.StandardCharsets
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model._
import org.slf4j.LoggerFactory
import io.openexchange.aws.s3.S3Client

object Application {
  private val csvInputSerialization = new InputSerialization().withCsv(new CSVInput().withFileHeaderInfo(FileHeaderInfo.USE)).withCompressionType(CompressionType.NONE)
  private val jsonOutputSerialization = new OutputSerialization().withJson(new JSONOutput)
  private val jsonInputSerialization = new InputSerialization().withCompressionType(CompressionType.NONE).withJson(new JSONInput().withType(JSONType.DOCUMENT))
  private val csvOutputSerialization = new OutputSerialization().withCsv(new CSVOutput())

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val bucketName = "test-select-aws-openexchange-io"
    val s3Client = new S3Client(AmazonS3ClientBuilder.standard()
      .withCredentials(new EnvironmentVariableCredentialsProvider)
      .build())
      s3Client.keys(bucketName, (t : S3ObjectSummary) => {
        logger.debug(t.getKey)
      })
    
    val csvFile = "users.csv"
    val selectFromCsvQuery = "select s.ID,s.LAST_NAME from S3Object s WHERE s.FIRST_NAME='David'"
    s3Client.select(bucketName, csvFile, selectFromCsvQuery, csvInputSerialization, jsonOutputSerialization, (event : SelectObjectContentEvent.RecordsEvent)=>{
      logger.debug(StandardCharsets.UTF_8.decode(event.getPayload).toString)
    })

    val jsonFile = "users.json"
    val selectFromJsonQuery = "select s.ID,s.LAST_NAME from S3Object[*].users[*] s WHERE s.FIRST_NAME='Alex'"
    s3Client.select(bucketName, jsonFile, selectFromJsonQuery, jsonInputSerialization, csvOutputSerialization,(event : SelectObjectContentEvent.RecordsEvent)=>{
      logger.debug(StandardCharsets.UTF_8.decode(event.getPayload).toString)
    })

    val noaaFile = "2287462.csv"
    val selectWeatherQuery = "select s.station,s.observationDate,s.reportType,s.observationSource,s.hourlyDewPointTemperature,s.hourlyDryBulbTemperature,s.hourlyPrecipitation,s.hourlyRelativeHumidity,s.hourlySeaLevelPressure,s.hourlySkyConditions,s.hourlyStationPressure,s.hourlyVisibility,s.hourlyWetBulbTemperature,s.hourlyWindDirection,s.hourlyWindGustSpeed,s.hourlyWindSpeed from s3object s limit 5;"
    s3Client.select(bucketName, noaaFile, selectWeatherQuery, csvInputSerialization, jsonOutputSerialization, (event : SelectObjectContentEvent.RecordsEvent)=>{
      logger.debug(StandardCharsets.UTF_8.decode(event.getPayload).toString)
    })
  }
}

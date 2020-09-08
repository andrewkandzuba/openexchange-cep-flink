package io.openexchange.aws.s3

import com.amazonaws.services.s3.model._

object Application {
  private val csvInputSerialization = new InputSerialization().withCsv(new CSVInput().withFileHeaderInfo(FileHeaderInfo.USE)).withCompressionType(CompressionType.NONE)
  private val jsonOutputSerialization = new OutputSerialization().withJson(new JSONOutput)
  private val jsonInputSerialization = new InputSerialization().withCompressionType(CompressionType.NONE).withJson(new JSONInput().withType(JSONType.DOCUMENT))
  private val csvOutputSerialization = new OutputSerialization().withCsv(new CSVOutput())

  def main(args: Array[String]): Unit = {
    val bucketName = "test-select-aws-openexchange-io"
    val s3Client = new S3Client
    s3Client.list(bucketName)


    val csvFile = "users.csv"
    val selectFromCsvQuery = "select s.ID,s.LAST_NAME from S3Object s WHERE s.FIRST_NAME='David'"
    val json = s3Client.select(bucketName, csvFile, selectFromCsvQuery, csvInputSerialization, jsonOutputSerialization)
    println(json)
    
    val jsonFile = "users.json"
    val selectFromJsonQuery = "select s.ID,s.LAST_NAME from S3Object[*].users[*] s WHERE s.FIRST_NAME='Alex'"
    val csv = s3Client.select(bucketName, jsonFile, selectFromJsonQuery, jsonInputSerialization, csvOutputSerialization)
    println(csv)
  }
}

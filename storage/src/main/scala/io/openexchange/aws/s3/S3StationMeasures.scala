package io.openexchange.aws.s3

import com.amazonaws.services.s3.model._
import io.openexchange.storage.StationMeasures

import java.nio.charset.StandardCharsets

case class S3StationMeasures(s3Client: S3Client, bucket: String, key: String) extends StationMeasures {
  private val csvInputSerialization = new InputSerialization().withCsv(new CSVInput().withFileHeaderInfo(FileHeaderInfo.USE)).withCompressionType(CompressionType.NONE)
  private val jsonOutputSerialization = new OutputSerialization().withJson(new JSONOutput)

  override def search(query: String): List[String] = {
    var list : List[String] = List()
    search(query, (t : SelectObjectContentEvent.RecordsEvent) => {
      list = StandardCharsets.UTF_8.decode(t.getPayload).toString :: list
    })
    list
  }

  private def search(query: String, process: SelectObjectContentEvent.RecordsEvent => Unit): Unit = {
    s3Client.select(bucket, key, query, csvInputSerialization, jsonOutputSerialization, process);
  }
}

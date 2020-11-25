package io.openexchange.aws.s3

import com.amazonaws.services.s3.model._
import io.openexchange.storage.StationMeasuresStore

case class S3StationMeasuresStore(s3Client: S3Client, bucket: String, key: String) extends StationMeasuresStore {
  private val csvInputSerialization = new InputSerialization().withCsv(new CSVInput().withFileHeaderInfo(FileHeaderInfo.USE)).withCompressionType(CompressionType.NONE)
  private val jsonOutputSerialization = new OutputSerialization().withJson(new JSONOutput)

  override def search(query: String, process: SelectObjectContentEvent.RecordsEvent => Unit): Unit = {
    s3Client.select(bucket, key, query, csvInputSerialization, jsonOutputSerialization, process);
  }
}

package io.openexchange.aws.s3

import io.openexchange.noaa.StationBearing
import io.openexchange.storage.StationStore

case class S3StationStore(s3Client: S3Client, bucket: String, key: String) extends StationStore {
  override def all: List[StationBearing] = List()

  override def search(query: String): List[StationBearing] = List()
}

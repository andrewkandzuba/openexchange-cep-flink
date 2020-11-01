package io.openexchange.aws.s3

import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class StationStoreTest extends AnyFlatSpec with MockFactory with Matchers {

  private val s3Client = mock[S3Client]
  private val stationStore = S3StationStore(s3Client, "testBucket", "testKey")

  "A StationStore" should "return of a List of size 0 when All method is called" in {
    val list = stationStore.all
    assert(list.isEmpty)
  }

  "A StationStore" should "return of a List of size 0 when search method is called" in {
    val list = stationStore.search("select * from stations")
    assert(list.isEmpty)
  }

}

package io.openexchange.aws.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{InputSerialization, ListObjectsV2Request, ListObjectsV2Result, OutputSerialization}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class S3ClientTest extends AnyFlatSpec with MockFactory {

  "A List" should "have size 0" in {
    val amazonS3Mock = mock[AmazonS3]
    (amazonS3Mock.listObjectsV2(_ : ListObjectsV2Request)).expects(*).returning(new ListObjectsV2Result).once()

    val s3Client = new S3Client(amazonS3Mock)
    val list = s3Client.list("bucketName")
    assert(list.isEmpty)
  }
  
  "An empty Set" should "have size 0" in {
    assert(Set.empty.isEmpty)
  }

  it should "produce NoSuchElementException when head is invoked" in {
    assertThrows[NoSuchElementException] {
      Set.empty.head
    }
  }
}

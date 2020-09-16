package com.amazonaws.internal

import java.io.ByteArrayInputStream
import java.util
import java.util.UUID

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.internal.InputSubstream
import com.amazonaws.services.s3.model._
import com.amazonaws.{AmazonServiceException, SdkClientException}
import io.openexchange.aws.s3.S3Client
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._

class S3ClientTest extends AnyFlatSpec with MockFactory with Matchers {

  "A List" should "have size 0" in {
    val amazonS3Mock = mock[AmazonS3]
    (amazonS3Mock.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(new ListObjectsV2Result).once()

    val s3Client = new S3Client(amazonS3Mock)
    val list = s3Client.list("bucketName")
    assert(list.isEmpty)
  }

  "A List" should "have size 2" in {
    val obj1 = new S3ObjectSummary
    obj1.setBucketName("bucket1")
    obj1.setSize(1000)

    val obj2 = new S3ObjectSummary
    obj2.setBucketName("bucket2")
    obj2.setSize(2000)

    val objList = new util.ArrayList[S3ObjectSummary]()
    objList.add(obj1)
    objList.add(obj2)

    val result = mock[ListObjectsV2Result]
    (result.getObjectSummaries _).expects().returning(objList).once();
    (result.isTruncated _).expects().returning(false).once()
    (result.getNextContinuationToken _).expects().returning(UUID.randomUUID().toString).once()

    val amazonS3Mock = mock[AmazonS3]
    (amazonS3Mock.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(result).once()

    val s3Client = new S3Client(amazonS3Mock)
    val list = s3Client.list("bucketName")
    assert(list.size == 2)
  }

  it should "produce AmazonServiceException when listObjectsV2 is invoked" in {
    intercept[AmazonServiceException] {
      val amazonS3Mock = mock[AmazonS3]
      (amazonS3Mock.listObjectsV2(_: ListObjectsV2Request)).expects(*).throws(new AmazonServiceException("Unauthorized")).once()

      val s3Client = new S3Client(amazonS3Mock)
      s3Client.list("bucketName")
    }
  }

  it should "produce SdkClientException when listObjectsV2 is invoked" in {
    intercept[SdkClientException] {
      val amazonS3Mock = mock[AmazonS3]
      (amazonS3Mock.listObjectsV2(_: ListObjectsV2Request)).expects(*).throws(new SdkClientException("Not found")).once()

      val s3Client = new S3Client(amazonS3Mock)
      s3Client.list("bucketName")
    }
  }

  "A List" should "have length 2" in {

    val payload = Array[Byte](0, 0, 0, -121, 0, 0, 0, 85, 125, -27, 22, -112, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45,
      116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82,
      101, 99, 111, 114, 100, 115, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108,
      105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 123, 34, 73, 68, 34, 58, 34,
      51, 34, 44, 34, 76, 65, 83, 84, 95, 78, 65, 77, 69, 34, 58, 34, 82, 105, 99, 104, 97, 114, 100, 115, 34, 125, 10, -13, 46,
      -38, -82, 0, 0, 0, -48, 0, 0, 0, 67, 3, 2, -80, 26, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0,
      5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 13, 58,
      99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 60, 83, 116, 97,
      116, 115, 32, 120, 109, 108, 110, 115, 61, 34, 34, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 49, 50,
      57, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115,
      115, 101, 100, 62, 49, 50, 57, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116,
      101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 51, 52, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100,
      62, 60, 47, 83, 116, 97, 116, 115, 62, -106, -63, 3, -102, 0, 0, 0, 56, 0, 0, 0, 40, -63, -58, -124, -44, 13, 58, 109, 101, 115,
      115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112,
      101, 7, 0, 3, 69, 110, 100, -49, -105, -45, -110, 0, 0, 0, 110, 0, 0, 0, 85, 88, 33, -77, 62, 13, 58, 109, 101, 115, 115, 97, 103,
      101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7,
      82, 101, 99, 111, 114, 100, 115, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108,
      105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 50, 44, 77, 105, 108, 108, 101,
      114, 10, -100, -6, 78, -76, 0, 0, 0, -49, 0, 0, 0, 67, -31, -78, -80, 73, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121,
      112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116,
      115, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 60, 83,
      116, 97, 116, 115, 32, 120, 109, 108, 110, 115, 61, 34, 34, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100,
      62, 52, 49, 53, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111,
      99, 101, 115, 115, 101, 100, 62, 52, 49, 53, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62,
      60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 57, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117,
      114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, -72, -54, 60, -55, 0, 0, 0, 56, 0, 0, 0, 40, -63, -58, -124, -44,
      13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101,
      110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, -49, -105, -45, -110)
    val selectObjectContentEventStream = new SelectObjectContentEventStream(new InputSubstream(new ByteArrayInputStream(payload), 0, payload.length, false))
    val selectObjectContentResult = new SelectObjectContentResult().withPayload(selectObjectContentEventStream)

    val amazonS3Mock = mock[AmazonS3]
    (amazonS3Mock.selectObjectContent(_: SelectObjectContentRequest)).expects(*).returning(selectObjectContentResult).once()

    val s3Client = new S3Client(amazonS3Mock)

    val bucketName = "test-select-aws-openexchange-io"
    val jsonFile = "users.json"
    val selectFromJsonQuery = "select s.ID,s.LAST_NAME from S3Object[*].users[*] s WHERE s.FIRST_NAME='Alex'"
    val list = s3Client.select(bucketName, jsonFile, selectFromJsonQuery, new InputSerialization, new OutputSerialization)

    assert(list.length == 2)
  }

  it should "produce Exception when select is invoked" in {

    val payload = Array[Byte](0, 0, 0, -121, 0, 0, 0, 85, 125, -27, 22, -112, 13, 58, 109, 101, 115, 115, 97, 103,
      101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112,
      101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101,
      7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101,
      97, 109, 123, 34, 73, 68, 34, 58, 34, 51, 34, 44, 34, 76, 65, 83, 84, 95, 78, 65, 77, 69, 34, 58, 34, 82, 105, 99,
      104, 97, 114, 100, 115, 34, 125, 10, -13, 46, -38, -82)
    val selectObjectContentEventStream = new SelectObjectContentEventStream(new InputSubstream(new ByteArrayInputStream(payload), 0, payload.length, false))
    val selectObjectContentResult = new SelectObjectContentResult().withPayload(selectObjectContentEventStream)

    val amazonS3Mock = mock[AmazonS3]
    (amazonS3Mock.selectObjectContent(_: SelectObjectContentRequest)).expects(*).returning(selectObjectContentResult).once()

    val s3Client = new S3Client(amazonS3Mock)

    val bucketName = "test-select-aws-openexchange-io"
    val jsonFile = "users.json"
    val selectFromJsonQuery = "select s.ID,s.LAST_NAME from S3Object[*].users[*] s WHERE s.FIRST_NAME='Alex'"

    intercept[Exception] {
      s3Client.select(bucketName, jsonFile, selectFromJsonQuery, new InputSerialization, new OutputSerialization)
    }
  }
}

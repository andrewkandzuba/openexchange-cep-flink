package io.openexchange.aws.s3

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util
import java.util.{Collections, UUID}

import com.amazonaws.internal.SdkFilterInputStream
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.internal.eventstreaming.Message
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.model.transform.SelectObjectContentEventUnmarshaller
import com.amazonaws.services.s3.model.transform.SelectObjectContentEventUnmarshaller.{EndEventUnmarshaller, RecordsEventUnmarshaller, StatsEventUnmarshaller}
import com.amazonaws.{AmazonServiceException, SdkClientException}
import jdk.internal.net.http.websocket.MessageEncoder
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._

class S3ClientTest extends AnyFlatSpec with MockFactory with Matchers {

  "A List" should "have size 0" in {
    val amazonS3Mock = mock[AmazonS3]
    (amazonS3Mock.listObjectsV2(_ : ListObjectsV2Request)).expects(*).returning(new ListObjectsV2Result).once()

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
    (amazonS3Mock.listObjectsV2(_ : ListObjectsV2Request)).expects(*).returning(result).once()

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
}

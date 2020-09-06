package io.openexchange.aws.s3

import java.util.function.Consumer

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, S3ObjectSummary}
import com.amazonaws.{AmazonServiceException, SdkClientException}

class S3Client() {

  private val s3Client = AmazonS3ClientBuilder.standard()
    .withCredentials(new EnvironmentVariableCredentialsProvider)
    .build()

  def list(bucketName: String): Unit = {
    try {
      val req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(2)
      var result: ListObjectsV2Result = null
      do {
        result = s3Client.listObjectsV2(req)
        result.getObjectSummaries.forEach(new Consumer[S3ObjectSummary] {
          override def accept(t: S3ObjectSummary): Unit = {
            printf(" - %s (size: %d)\n", t.getKey, t.getSize)
            println()
          }
        })

        // If there are more than maxKeys keys in the bucket, get a continuation token
        // and list the next objects.
        val token = result.getNextContinuationToken;
        System.out.println("Next Continuation Token: " + token);
        req.setContinuationToken(token);

      } while (result.isTruncated)
    } catch {
      case e: AmazonServiceException => e.printStackTrace()
      case e: SdkClientException => e.printStackTrace()
    }
  }
}

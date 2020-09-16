package io.openexchange.aws.s3

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{InputSerialization, OutputSerialization, SelectObjectContentEvent, SelectObjectContentEventVisitor, _}
import com.amazonaws.{AmazonServiceException, SdkClientException}

import scala.collection.mutable.ListBuffer

class S3Client(val amazonS3: AmazonS3) {

  def list(bucketName: String): List[String] = {
    val list = new ListBuffer[String]()
    try {
      val req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(2)
      var result: ListObjectsV2Result = null
      do {
        result = amazonS3.listObjectsV2(req)
        result.getObjectSummaries.forEach(new Consumer[S3ObjectSummary] {
          override def accept(t: S3ObjectSummary): Unit = {
            list += " - %s (size: %d)".format(t.getKey, t.getSize)
          }
        })

        // If there are more than maxKeys keys in the bucket, get a continuation token
        // and list the next objects.
        val token = result.getNextContinuationToken;
        System.out.println("Next Continuation Token: " + token);
        req.setContinuationToken(token);

      } while (result.isTruncated)
    } catch {
      case e: AmazonServiceException => e.printStackTrace(); throw e
      case e: SdkClientException => e.printStackTrace(); throw e
    }
    list.toList
  }

  def select(bucketName: String, key: String, query: String, inputSerialization: InputSerialization, outputSerialization: OutputSerialization): List[String] = {
    val output = new ListBuffer[String]()

    println(query)

    val response = amazonS3.selectObjectContent(new SelectObjectContentRequest()
      .withBucketName(bucketName)
      .withKey(key)
      .withExpression(query)
      .withExpressionType(ExpressionType.SQL)
      .withInputSerialization(inputSerialization)
      .withOutputSerialization(outputSerialization))
    val isResultComplete = new AtomicBoolean(false)

    try {
      def inputStream = response.getPayload.getRecordsInputStream(
        new SelectObjectContentEventVisitor() {
          override def visit(event: SelectObjectContentEvent.StatsEvent): Unit = {
            println("Received Stats, Bytes Scanned: " + event.getDetails.getBytesScanned + " Bytes Processed: " + event.getDetails.getBytesProcessed)
          }

          override def visit(event: SelectObjectContentEvent.EndEvent): Unit = {
            isResultComplete.set(true)
            println("Received End Event. Result is complete.")
          }

          override def visit(event: SelectObjectContentEvent.RecordsEvent): Unit = {
            val s = StandardCharsets.UTF_8.decode(event.getPayload).toString
            println("Received record: " + s)
            output += s
          }
        })

      scala.io.Source.fromInputStream(inputStream).mkString
    }
    finally response.close()

    if (!isResultComplete.get) throw new Exception("S3 Select request was incomplete as End Event was not received.")

    output.toList
  }
}

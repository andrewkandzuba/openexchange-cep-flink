package io.openexchange.aws.s3

import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{CSVInput, CompressionType, InputSerialization, OutputSerialization, SelectObjectContentEvent, SelectObjectContentEventVisitor, _}
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

  def select(bucketName: String, key: String, query: String, inputSerialization: InputSerialization, outputSerialization: OutputSerialization) : String = {
    println(query)
    val response = s3Client.selectObjectContent(new SelectObjectContentRequest()
        .withBucketName(bucketName)
        .withKey(key)
        .withExpression(query)
        .withExpressionType(ExpressionType.SQL)
        .withInputSerialization(inputSerialization)
        .withOutputSerialization(outputSerialization))
    val isResultComplete = new AtomicBoolean(false)

    val output : String = try {
      def inputStream = response.getPayload.getRecordsInputStream(
        new SelectObjectContentEventVisitor() {
          override def visit(event: SelectObjectContentEvent.StatsEvent): Unit = {
            println("Received Stats, Bytes Scanned: " + event.getDetails.getBytesScanned + " Bytes Processed: " + event.getDetails.getBytesProcessed)
          }

          override def visit(event: SelectObjectContentEvent.EndEvent): Unit = {
            isResultComplete.set(true)
            println("Received End Event. Result is complete.")
          }
        })
      scala.io.Source.fromInputStream(inputStream).mkString
    }
    finally response.close()

    if (!isResultComplete.get) throw new Exception("S3 Select request was incomplete as End Event was not received.")

    output
  }

  /*.withInputSerialization((new InputSerialization)
        .withCsv((new CSVInput).withFileHeaderInfo(FileHeaderInfo.USE))
        .withCompressionType(CompressionType.NONE))
      .withOutputSerialization((new OutputSerialization)
        .withJson(new JSONOutput))*/
}

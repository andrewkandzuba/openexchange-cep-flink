package io.openexchange.flink.function

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model._
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

abstract class S3SelectSourceFunction[OUT](bucketName: String, key: String, query: String) extends RichSourceFunction[OUT] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val csvInputSerialization = new InputSerialization().withCsv(new CSVInput().withFileHeaderInfo(FileHeaderInfo.USE)).withCompressionType(CompressionType.NONE)
  private val jsonOutputSerialization = new OutputSerialization().withJson(new JSONOutput)

  @volatile private var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[OUT]): Unit = {
    val response = AmazonS3ClientBuilder.standard()
      .withCredentials(new EnvironmentVariableCredentialsProvider)
      .build()
      .selectObjectContent(new SelectObjectContentRequest()
        .withBucketName(bucketName)
        .withKey(key)
        .withExpression(query)
        .withExpressionType(ExpressionType.SQL)
        .withInputSerialization(csvInputSerialization)
        .withOutputSerialization(jsonOutputSerialization))
    val isResultComplete = new AtomicBoolean(false)

    try {
      def iterator = response.getPayload.getEventsIterator

      while (isRunning && iterator.hasNext) {
        val event = iterator.next()
        event.visit(new SelectObjectContentEventVisitor() {
          override def visit(event: SelectObjectContentEvent.StatsEvent): Unit = {
            logger.debug("Received Stats, Bytes Scanned: {}, Bytes Processed: {}", event.getDetails.getBytesScanned, event.getDetails.getBytesProcessed: Any)
          }

          override def visit(event: SelectObjectContentEvent.EndEvent): Unit = {
            logger.debug("Received End Event. Result is complete.")
            isResultComplete.set(true)
          }

          override def visit(event: SelectObjectContentEvent.RecordsEvent): Unit = {
            logger.debug("Received record.")
            sourceContext.collect(deserialize(event.getPayload))
          }
        })
      }
    }
    finally response.close()

    if (!isResultComplete.get) throw new Exception("S3 Select request was incomplete as End Event was not received.")

  }

  override def cancel(): Unit = {
    isRunning = false
  }

  def deserialize(payload : ByteBuffer) : OUT
}

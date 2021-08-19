package io.openexchange.flink.function

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model._
import org.apache.flink.configuration.{ConfigOptions, Configuration}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean

class S3SelectSourceFunction extends RichSourceFunction[String] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val csvInputSerialization = new InputSerialization().withCsv(new CSVInput().withFileHeaderInfo(FileHeaderInfo.USE)).withCompressionType(CompressionType.NONE)
  private val jsonOutputSerialization = new OutputSerialization().withJson(new JSONOutput)
  private val BUCKET_NAME = "s3-bucket-name"
  private val FILE_NAME = "s3-file-name"
  private val QUERY = "s3-query"

  @volatile private var isRunning = true
  private var bucketName: String = _
  private var key: String = _
  private var query: String = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val taskName = getRuntimeContext.getTaskName

    bucketName = parameters.getString(ConfigOptions.key(taskName + "-" + BUCKET_NAME).stringType().noDefaultValue())
    key = parameters.getString(ConfigOptions.key(taskName + "-" + FILE_NAME).stringType().noDefaultValue())
    query = parameters.getString(ConfigOptions.key(taskName + "-" + QUERY).stringType().noDefaultValue())
  }

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
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
            sourceContext.collect(StandardCharsets.UTF_8.decode(event.getPayload).toString)
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
}

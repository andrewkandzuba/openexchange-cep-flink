package io.openexchange.flink.function

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.testutils.OneShotLatch
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.operators.StreamSource
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.JavaConversions

class S3SelectSourceFunctionTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  "S3SelectSourceFunction" should "produce events" in {

    val monitoringFunction = new S3SelectSourceFunction[String]("bucket", "key", "SELECT * FROM s3object s LIMIT 5") {
      override def deserialize(payload: ByteBuffer): String = {
        StandardCharsets.UTF_8.decode(payload).toString
      }
    }

    val src = new StreamSource[String, S3SelectSourceFunction[String]](monitoringFunction)

    val testHarness = new AbstractStreamOperatorTestHarness(src, new MockEnvironmentBuilder()
      .setParallelism(1)
      .setMaxParallelism(1)
      .setSubtaskIndex(0)
      .build())

    testHarness.getExecutionConfig.setAutoWatermarkInterval(50)

    val properties : java.util.Map[String, String] = JavaConversions.mapAsJavaMap(Map("AL" -> "Alabama", "AK" -> "Alaska"))
    val parameterTool = ParameterTool.fromMap(properties)
    testHarness.getExecutionConfig.setGlobalJobParameters(parameterTool)

    testHarness.setup()
    testHarness.open()

    val error = new Array[Throwable](1)
    val latch = new OneShotLatch()

    val sourceContext = new DummySourceContext {
      override def collect(t: String): Unit = {
        latch.trigger()
      }
    }

    val runner = new Thread() {
      override def run(): Unit = {
        try {
          monitoringFunction.run(sourceContext)
        } catch {
          case e: Throwable =>
            e.printStackTrace()
            error(0) = e
            latch.trigger()
        }
      }
    }
    runner.start()

    if (!latch.isTriggered) {
      latch.await()
    }

    synchronized(sourceContext.getCheckpointLock())

    monitoringFunction.cancel();
    runner.join();

    testHarness.close();

    error should have length 0
  }

  private class DummySourceContext extends SourceFunction.SourceContext[String] {
    private val lock = new Object

    override def collect(t: String): Unit = {
    }

    override def collectWithTimestamp(t: String, l: Long): Unit = {
    }

    override def emitWatermark(watermark: Watermark): Unit = {
    }

    override def markAsTemporarilyIdle(): Unit = {
    }

    override def getCheckpointLock: AnyRef = {
      lock
    }

    override def close(): Unit = {

    }
  }

}

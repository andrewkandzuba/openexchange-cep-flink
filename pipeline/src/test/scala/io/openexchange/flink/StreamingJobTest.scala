package io.openexchange.flink

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit

class StreamingJobTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  "StatefulFlatMapFunction pipeline" should "incrementValues" in {
    StreamingJob.main(Array())
    Thread.sleep(TimeUnit.SECONDS.toMillis(5))
    // verify your results
    CollectSink.values should contain allOf(2, 22, 23)
  }
}

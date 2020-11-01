package io.openexchange.flink

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StreamingJobIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }

  "IncrementFlatMapFunction pipeline" should "incrementValues" in {
    StreamingJob.main(Array())
    Thread.sleep(10000)
    // verify your results
    CollectSink.values should contain allOf(2, 22, 23)
  }
}

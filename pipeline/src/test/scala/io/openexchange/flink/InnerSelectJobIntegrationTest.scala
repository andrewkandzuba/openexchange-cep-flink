package io.openexchange.flink

import io.openexchange.flink.data.{InstrumentsSink, UnderlayersSink}
import io.openexchange.flink.model.{Instrument, Underlayer}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class InnerSelectJobIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(2)
    .setNumberTaskManagers(2)
    .build)

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }

  "UnderlayFlatMapFunction" should "flatMap" in {
    UnderlayersSink.values.clear()
    InnerSelectJob.main(Array())
    Thread.sleep(10000)
    UnderlayersSink.values should contain allOf(Underlayer.apply(1), Underlayer.apply(3))
    InstrumentsSink.values should contain allOf(Instrument.apply(1, 0), Instrument.apply(3, 4))
  }
}

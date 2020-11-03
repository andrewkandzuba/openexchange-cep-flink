package io.openexchange.flink

import io.openexchange.flink.data.{CollectionSinkFactory, FiniteSourceFactory, InstrumentsSink}
import io.openexchange.flink.model.Instrument
import io.openexchange.flink.model.InstrumentType.{EQUITY, INDEX}
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
    InstrumentsSink.values.clear()
    InnerSelectJob(new FiniteSourceFactory, new CollectionSinkFactory).run()
    Thread.sleep(10000)
    InstrumentsSink.values should contain allOf(Instrument.apply(1, 0, EQUITY), Instrument.apply(3, 4, INDEX))
  }
}

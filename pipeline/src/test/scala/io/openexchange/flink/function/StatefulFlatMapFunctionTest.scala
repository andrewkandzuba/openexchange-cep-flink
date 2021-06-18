package io.openexchange.flink.function

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.operators.StreamFlatMap
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StatefulFlatMapFunctionTest extends AnyFlatSpec with Matchers with BeforeAndAfter  {
  private var testHarness: OneInputStreamOperatorTestHarness[Long, Long] = _
  private var statefulFlatMap: FlatMapFunction[Long, Long] = _

  before {
    //instantiate user-defined function
    statefulFlatMap = new StatefulFlatMapFunction

    // wrap user defined function into a the corresponding operator
    testHarness = new OneInputStreamOperatorTestHarness[Long, Long](new StreamFlatMap(statefulFlatMap))

    // optionally configured the execution environment
    testHarness.getExecutionConfig.setAutoWatermarkInterval(50);

    // open the test harness (will also call open() on RichFunctions)
    testHarness.open();
  }

  "StatefulFlatMap" should "do some fancy stuff with timers and state" in {


    //push (timestamped) elements into the operator (and hence user defined function)
    testHarness.processElement(2, 100);

    //trigger event time timers by advancing the event time of the operator with a watermark
    testHarness.processWatermark(100);

    //trigger proccesign time timers by advancing the processing time of the operator directly
    testHarness.setProcessingTime(100);

    //retrieve list of emitted records for assertions
    testHarness.extractOutputValues() should contain (3)

    //retrieve list of records emitted to a specific side output for assertions (ProcessFunction only)
    //testHarness.getSideOutput(new OutputTag[Int]("invalidRecords")) should have size 0
  }

}

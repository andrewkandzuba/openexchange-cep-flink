package io.openexchange.flink.function

import io.openexchange.flink.model.{Instrument, Underlay}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

class InstrumentAndUnderlayJoiner extends RichCoFlatMapFunction[Instrument, Underlay, Instrument] {

  lazy val instrumentState: ValueState[Instrument] =
    getRuntimeContext.getState(new ValueStateDescriptor[Instrument]("instrumentState", classOf[Instrument]))
  lazy val underlayState: ValueState[Underlay] =
    getRuntimeContext.getState(new ValueStateDescriptor[Underlay]("underlayState", classOf[Underlay]))

  override def flatMap1(in1: Instrument, collector: Collector[Instrument]): Unit = {
    val underlay = underlayState.value();
    if (underlay != null) {
      underlayState.clear()
      collector.collect(in1)
    } else
      instrumentState.update(in1)
  }

  override def flatMap2(in2: Underlay, collector: Collector[Instrument]): Unit = {
    val instrument = instrumentState.value()
    if (instrument != null) {
      instrumentState.clear()
      collector.collect(instrument)
    } else {
      underlayState.update(in2)
    }
  }
}

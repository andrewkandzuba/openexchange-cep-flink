package io.openexchange.flink.function

import io.openexchange.flink.model.{Instrument, Underlayer}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

class InstrumentAndUnderlayJoiner extends RichCoFlatMapFunction[Instrument, Underlayer, Instrument] {

  lazy val instrumentState: ValueState[Instrument] =
    getRuntimeContext.getState(new ValueStateDescriptor[Instrument]("instrumentState", classOf[Instrument]))
  lazy val underlayerState: ValueState[Underlayer] =
    getRuntimeContext.getState(new ValueStateDescriptor[Underlayer]("underlayerState", classOf[Underlayer]))

  override def flatMap1(instrument: Instrument, collector: Collector[Instrument]): Unit = {
    val underlayer = underlayerState.value();
    if (underlayer != null) {
      underlayerState.clear()
      collector.collect(instrument)
    } else
      instrumentState.update(instrument)
  }

  override def flatMap2(underlayer: Underlayer, collector: Collector[Instrument]): Unit = {
    val instrument = instrumentState.value()
    if (instrument != null) {
      instrumentState.clear()
      collector.collect(instrument)
    } else {
      underlayerState.update(underlayer)
    }
  }
}

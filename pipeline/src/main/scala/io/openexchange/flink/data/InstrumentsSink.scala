package io.openexchange.flink.data

import java.util

import io.openexchange.flink.model.Instrument
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class InstrumentsSink extends SinkFunction[Instrument] {

  override def invoke(value: Instrument, context: SinkFunction.Context[_]): Unit = {
    InstrumentsSink.values.add(value)
  }
}

object InstrumentsSink {
  val values: util.List[Instrument] = new util.ArrayList()
}
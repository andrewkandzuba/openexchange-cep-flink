package io.openexchange.flink.data

import java.util

import io.openexchange.flink.model.Underlay
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class UnderlaysSink extends SinkFunction[Underlay] {

  override def invoke(value: Underlay, context: SinkFunction.Context[_]): Unit = {
    UnderlaysSink.values.add(value)
  }
}

object UnderlaysSink {
  val values: util.List[Underlay] = new util.ArrayList()
}

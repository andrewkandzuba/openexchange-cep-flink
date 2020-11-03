package io.openexchange.flink.data

import java.util
import java.util.Collections

import io.openexchange.flink.model.Underlayer
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class UnderlayersSink extends SinkFunction[Underlayer] {

  override def invoke(value: Underlayer, context: SinkFunction.Context[_]): Unit = {
    UnderlayersSink.values.add(value)
  }
}

object UnderlayersSink {
  val values: util.List[Underlayer] = Collections.synchronizedList(new util.ArrayList())
}

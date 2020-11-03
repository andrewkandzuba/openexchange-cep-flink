package io.openexchange.flink.data

import io.openexchange.flink.model.Instrument
import org.apache.flink.streaming.api.functions.sink.SinkFunction

trait SinkFactory {
  def instruments(): SinkFunction[Instrument]
}

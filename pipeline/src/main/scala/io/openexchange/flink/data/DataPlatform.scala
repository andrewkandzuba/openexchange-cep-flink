package io.openexchange.flink.data

import io.openexchange.flink.model.{Instrument, Underlay}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

object DataPlatform {
  def instrumentSource : List[Instrument] = List(
    Instrument(1, 0),
    Instrument(2, 1, isSpecial = true),
    Instrument(3, 4),
    Instrument(4, 0, isSpecial = true),
    Instrument(5, 3, isSpecial = true),
    Instrument(6, 1, isSpecial = true)
  )

  def underlaysSink(): SinkFunction[Underlay] = new UnderlaysSink

  def instrumentsSink() : SinkFunction[Instrument] = new InstrumentsSink
}


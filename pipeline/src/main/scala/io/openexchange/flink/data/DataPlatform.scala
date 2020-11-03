package io.openexchange.flink.data

import io.openexchange.flink.model.{Instrument, Underlay}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.functions.sink.SinkFunction

object DataPlatform {
  def instrumentSource : List[Instrument] = List(
    Instrument(1, 0),
    Instrument(2, 1, isSpecial = true),
    Instrument(3, 4),
    Instrument(4, 0, isSpecial = true),
    Instrument(5, 3, isSpecial = true)
  )

  def underlaysSink(): SinkFunction[Underlay] = new UnderlaysSink

  def instrumentsSink() : SinkFunction[Instrument] = new InstrumentsSink

  def instrumentKeySelector: KeySelector[Instrument, Int] = {
    new KeySelector[Instrument, Int] {
      override def getKey(value: Instrument): Int = value.id
    }
  }

  def underlayKeySelector: KeySelector[Underlay, Int] = {
    new KeySelector[Underlay, Int] {
      override def getKey(value: Underlay): Int = value.id
    }
  }
}


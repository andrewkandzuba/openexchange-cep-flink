package io.openexchange.flink.data
import io.openexchange.flink.model.Instrument
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class CollectionSinkFactory extends SinkFactory {
  override def instruments(): SinkFunction[Instrument] = new InstrumentsSink
}

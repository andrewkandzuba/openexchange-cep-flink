package io.openexchange.flink.data
import io.openexchange.flink.model.Instrument
import org.apache.flink.streaming.api.functions.source.SourceFunction

class DumpSourceFactory extends SourceFactory {
  override def instruments(): SourceFunction[Instrument] = ???
}

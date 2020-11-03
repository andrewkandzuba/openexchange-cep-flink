package io.openexchange.flink.data
import io.openexchange.flink.model.Instrument
import io.openexchange.flink.model.InstrumentType.{EQUITY, INDEX, OPTION, WARRANT}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.util.FiniteTestSource

class FiniteSourceFactory extends SourceFactory {
  override def instruments(): SourceFunction[Instrument] = new FiniteTestSource (
    Instrument(1, 0, EQUITY),
    Instrument(2, 1, WARRANT),
    Instrument(3, 4, INDEX),
    Instrument(4, 0, WARRANT),
    Instrument(5, 3, OPTION),
    Instrument(6, 1, WARRANT),
    Instrument(7, 3, OPTION),
    Instrument(8, 11, WARRANT)
  )
}

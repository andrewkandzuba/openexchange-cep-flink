package io.openexchange.flink

import io.openexchange.flink.data.{DumpSinkFactory, DumpSourceFactory, SinkFactory, SourceFactory}
import io.openexchange.flink.function.InstrumentAndUnderlayJoiner
import io.openexchange.flink.model.{Instrument, InstrumentType, Underlayer}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

case class InnerSelectJob(sourceFactory: SourceFactory, sinkFactory: SinkFactory) {
  def run() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val instruments: DataStream[Instrument] = env.addSource(sourceFactory.instruments())

    // create flat map stream of underlayers
    val underlayersByKey: KeyedStream[Underlayer, Int] = instruments
      .filter(r => (InstrumentType.WARRANT.equals(r.instrumentType) || InstrumentType.OPTION.equals(r.instrumentType)) && r.underlayerId != 0)
      .map(value => Underlayer.apply(value.underlayerId))
      .keyBy(r => r.id)
    val instrumentsByKey: KeyedStream[Instrument, Int] = instruments.keyBy(r => r.id)

    // join instruments and underlayers and extract real instruments for underlays
    val underlyingInstruments = instrumentsByKey
      .connect(underlayersByKey)
      .flatMap(new InstrumentAndUnderlayJoiner)
      .uid("join-and-extract")

    underlyingInstruments.addSink(sinkFactory.instruments())

    // execute program
    env.executeAsync("Flink Streaming Scala API Skeleton")
  }
}

object InnerSelectJob extends App {
   new InnerSelectJob(new DumpSourceFactory, new DumpSinkFactory).run()
}

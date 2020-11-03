package io.openexchange.flink

import io.openexchange.flink.data.DataPlatform.{instrumentSource, instrumentsSink, underlayersSink}
import io.openexchange.flink.function.InstrumentAndUnderlayJoiner
import io.openexchange.flink.model.{Instrument, Underlayer}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object InnerSelectJob extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)

  val instruments: DataStream[Instrument] = env.fromCollection(instrumentSource)
  val sink = underlayersSink()
  val underlyingInstrumentsSink = instrumentsSink()

  // create flat map stream of underlayers
  val underlayersByKey: KeyedStream[Underlayer, Int] = instruments
    .filter(r => r.isSpecial && r.underlayerId != 0)
    .map(value => Underlayer.apply(value.underlayerId))
    .keyBy(r => r.id)
  val instrumentsByKey: KeyedStream[Instrument, Int] = instruments.keyBy(r => r.id)

  // join instruments and underlayers and extract real instruments for underlays
  val underlyingInstruments = instrumentsByKey
    .connect(underlayersByKey)
    .flatMap(new InstrumentAndUnderlayJoiner)
    .uid("join-and-extract")

  underlayersByKey.addSink(sink)
  underlyingInstruments.addSink(underlyingInstrumentsSink)

  // execute program
  env.executeAsync("Flink Streaming Scala API Skeleton")
}


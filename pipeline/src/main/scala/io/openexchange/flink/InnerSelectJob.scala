package io.openexchange.flink

import io.openexchange.flink.data.DataPlatform.{instrumentSource, instrumentsSink, underlaysSink}
import io.openexchange.flink.function.{InstrumentAndUnderlayJoiner, UnderlayFlatMapFunction}
import io.openexchange.flink.model.{Instrument, Underlay}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object InnerSelectJob extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)

  val instruments: DataStream[Instrument] = env.fromCollection(instrumentSource)
  val sink = underlaysSink()
  val underlyingInstrumentsSink = instrumentsSink()

  // create flat map stream of underlays
  val underlaysByKey : KeyedStream[Underlay, Int] = instruments.flatMap(new UnderlayFlatMapFunction).keyBy(r => r.id)
  val instrumentsByKey : KeyedStream[Instrument, Int] = instruments.keyBy(r => r.id)

  // join instruments and underlays and extract real instruments for underlays
  val underlyingInstruments = instrumentsByKey
    .connect(underlaysByKey)
    .flatMap(new InstrumentAndUnderlayJoiner)
    .uid("joinAndFilter")

  underlaysByKey.addSink(sink)
  underlyingInstruments.addSink(underlyingInstrumentsSink)

  // execute program
  env.executeAsync("Flink Streaming Scala API Skeleton")
}


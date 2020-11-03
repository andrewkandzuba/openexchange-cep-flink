package io.openexchange.flink

import io.openexchange.flink.data.DataPlatform.{instrumentKeySelector, instrumentSource, instrumentsSink, underlayKeySelector, underlaysSink}
import io.openexchange.flink.function.{InstrumentAndUnderlayJoiner, UnderlayFlatMapFunction}
import io.openexchange.flink.model.Instrument
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object InnerSelectJob extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)

  val instruments: DataStream[Instrument] = env.fromCollection(instrumentSource)
  val sink = underlaysSink()
  val underlyingInstrumentsSink = instrumentsSink()

  // create flat map stream of underlays
  val underlays = instruments.flatMap(new UnderlayFlatMapFunction)

  // join instruments and underlays and extract real instruments for underlays
  val underlyingInstruments = instruments.keyBy(instrumentKeySelector)
    .connect(underlays.keyBy(underlayKeySelector))
    .flatMap(new InstrumentAndUnderlayJoiner)
    .uid("joinAndFilter")

  underlays.addSink(sink)
  underlyingInstruments.addSink(underlyingInstrumentsSink)

  // execute program
  env.executeAsync("Flink Streaming Scala API Skeleton")
}


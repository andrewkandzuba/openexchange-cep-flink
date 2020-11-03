package io.openexchange.flink.function

import io.openexchange.flink.model.{Instrument, Underlay}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class UnderlayFlatMapFunction extends FlatMapFunction[Instrument, Underlay] {
  override def flatMap(value: Instrument, out: Collector[Underlay]): Unit = {
    if (value.isSpecial && value.underlayId != 0) {
      out.collect(Underlay.apply(value.underlayId))
    }
  }
}

package io.openexchange.flink.function

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


class StatefulFlatMapFunction extends FlatMapFunction[Long, Long] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def flatMap(t: Long, collector: Collector[Long]): Unit = {
    logger.info("Receive %i", t)
    collector.collect(t+1)
  }
}

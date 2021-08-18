/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openexchange.flink

import io.openexchange.flink.function.StatefulFlatMapFunction

import java.util
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

import java.util.Collections

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob extends App {
  // set up the streaming execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(2)

  // values are collected in a static variable
  CollectSink.values.clear()

  // create a stream of custom elements and apply transformations
  env.fromElements(1L, 21L, 22L)
    .flatMap(new StatefulFlatMapFunction())
    .addSink(new CollectSink())

  // execute program
  env.execute("Flink Streaming Scala API Skeleton")
}

// create a testing sink
class CollectSink extends SinkFunction[Long] {

  override def invoke(value: Long, context: SinkFunction.Context): Unit = {
    CollectSink.values.add(value)
  }
}

object CollectSink {
  // must be static
  val values: util.List[Long] = Collections.synchronizedList(new util.ArrayList())
}
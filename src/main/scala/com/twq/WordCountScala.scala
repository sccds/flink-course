package com.twq

import org.apache.flink.streaming.api.scala._

object WordCountScala {
  def main(args: Array[String]): Unit = {
    // 1. 初始化流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 2. data source
    val dataStreamSource = env.socketTextStream("localhost", 5001)

    // 3. data process
    val wordCounts = dataStreamSource
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .keyBy(0)
      .sum(1)

    // 4. data sink
    wordCounts.print()

    env.execute("Streaming WordCount Scala")
  }
}

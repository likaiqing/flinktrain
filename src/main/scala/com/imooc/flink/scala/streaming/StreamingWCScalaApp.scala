package com.imooc.flink.scala.streaming

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWCScalaApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var port = 0

    val text = env.socketTextStream("localhost", 9999)
    text.flatMap(_.toLowerCase.split("\\s") filter (_.nonEmpty))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .map(t => t._1 + "-->" + t._2)
      .print()
      .setParallelism(1)

    val result: JobExecutionResult = env.execute("StreamingWCScalaApp")
  }

}

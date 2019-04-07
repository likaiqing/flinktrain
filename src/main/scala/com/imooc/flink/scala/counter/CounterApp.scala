package com.imooc.flink.scala.counter


import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

/**
  * 1,定义累加器(计数器)
  * 2,注册计数器
  * 3,获取注册器
  */
object CounterApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")
    //    data.map(new RichMapFunction[String, Long] {
    //      var counter = 0l
    //      override def map(value: String): Long = {
    //        counter = counter + 1;
    //        println("counter:" + counter)
    //        counter
    //      }
    //    }).setParallelism(3).print()
    //    data.print()

    val info = data.map(new RichMapFunction[String, String] {

      //1,定义累加器(计数器)
      var counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        //2,注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)
      }

      override def map(value: String): String = {
        counter.add(1)
        value
      }
    })
    //    info.print()
    val filePath = "file:///Users/likaiqing/space/learn/flinktrain/result"
    info.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(5)
    val jobResult = env.execute("CounterApp")
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")
    //3,获取注册器
    println("num:" + num)
  }
}

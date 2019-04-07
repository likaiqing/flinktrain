package com.imooc.flink.scala

/*
需要import org.apache.flink.api.scala._ 算子map,flatmap等会报错could not find implicit value for evidence paramter of type org.apache.flink.api.common.typeinfo.TypeInformation[String],是因为算子中，程序需要一个隐式参数(implicit parameter)，详看源码,
官方推荐使用import org.apache.flink.api.scala._(有界数据)或import org.apache.flink.streaming.api.scala._(无界数据)
 */

//引入隐式转换
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

object BatchWCScalaApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = "file:///Users/likaiqing/space/learn/flinktrain/test.csv"
    val text = env.readTextFile(input)
    text.print()
    text.map(s => s + ";").print()
    val counts = text.flatMap {
      _.toLowerCase.split("\\s+") filter {
        _.nonEmpty
      }
    }.map {
      (_, 1)
    }
      .groupBy(0)
      .sum(1)
    counts.writeAsText("/Users/likaiqing/space/learn/flinktrain/result",FileSystem.WriteMode.NO_OVERWRITE)
    counts.print()
  }

}

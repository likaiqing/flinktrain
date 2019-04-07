package com.imooc.flink.scala.sink

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

object DataSetSinkApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = 1.to(10)
    val text = env.fromCollection(data)
    val filePath = "file:///Users/likaiqing/space/learn/flinktrain/result"
    text.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1)//默认情况下，不设置并行度，写入一个文件，设置大于1 的并行度，会写入目录
    env.execute("DataSetSinkApp")
  }

}

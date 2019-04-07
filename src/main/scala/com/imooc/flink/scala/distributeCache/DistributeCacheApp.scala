package com.imooc.flink.scala.distributeCache

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * 分布式缓存
  * 1,注册一个本地/HDFS文件
  * 2,在open方法中获取到分布式缓存的内容即可
  */
object DistributeCacheApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filePath = "/Users/likaiqing/space/learn/flinktrain/test.csv"
    //1,注册一个本地/HDFS文件
    env.registerCachedFile(filePath, "pk-scala-dc")
    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

    data.map(new RichMapFunction[String, String] {
      //2,在open方法中获取到分布式缓存的内容即可
      override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache().getFile("pk-scala-dc")
        val lines = FileUtils.readLines(dcFile) //java 的list
        /**
          * 此时会出现一个异常：Java集合和Scala集合不兼容的问题
          */
        import scala.collection.JavaConverters._
        for (ele <- lines.asScala) {//for (ele <- lines)为scala类型
          println("---"+ele)
        }
      }
      override def map(value: String): String = {
        value
      }
    }).print()

  }

}

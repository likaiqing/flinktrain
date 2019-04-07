package com.imooc.flink.scala.source

import com.imooc.flink.scala.Person
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object DataSetDataSourceApp {

  def csvFile(env: ExecutionEnvironment): Unit = {
    val filePath = "file:///Users/likaiqing/space/learn/flinktrain/people.csv"

    //    env.readCsvFile[(String, Int, String)](filePath, ignoreFirstLine = true).print()

    //只取两列
    //    env.readCsvFile[(String, Int)](filePath, ignoreFirstLine = true).print()
    //    env.readCsvFile[(Int, String)](filePath, ignoreFirstLine = true, includedFields = Array(2, 1)).print()//includedFields = Array(2, 1)有顺序

    //case class
    case class MyCaseClass(name: String, age: Int)
    env.readCsvFile[MyCaseClass](filePath, ignoreFirstLine = true, includedFields = Array(0, 1)).print()

    //java POJO
    env.readCsvFile[Person](filePath, ignoreFirstLine = true, pojoFields = Array("name", "age", "work")).print()
  }

  def textFile(env: ExecutionEnvironment): Unit = {
    //    env.readTextFile("file:///Users/likaiqing/space/learn/flinktrain/test.csv")
    //      .print()

    env.readTextFile("file:///Users/likaiqing/space/learn/flinktrain/result")
      .print()
  }

  def fromCollection(env: ExecutionEnvironment): Unit = {
    val data = 1 to 10
    env.fromCollection(data).print()
  }

  /**
    * 读取目录递归的文件
    *
    * @param env
    */
  def readRecursiveFiles(env: ExecutionEnvironment): Unit = {
    val filePath = "/Users/likaiqing/space/learn/flinktrain/recursiveDir"
    env.readTextFile(filePath).print()
    println("-------------")
    val paramters = new Configuration()
    paramters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(filePath).withParameters(paramters).print()
  }

  def readCompressionFiles(env: ExecutionEnvironment): Unit = {
    val filePath = "/Users/likaiqing/space/learn/flinktrain/compressionDir"
    env.readTextFile(filePath).print()
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    fromCollection(env)

    //    textFile(env)

    //    csvFile(env)

    //    readRecursiveFiles(env)

    readCompressionFiles(env)
  }
}

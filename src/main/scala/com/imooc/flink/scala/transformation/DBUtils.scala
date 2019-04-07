package com.imooc.flink.scala.transformation

import scala.util.Random

object DBUtils {
  def getConnection() = {
    new Random().nextInt(10) + ""
  }

  def returnConnection(): Unit = {

  }

}

package com.gjing.projects.flink.dataset

import org.apache.flink.api.scala._

/**
 * @author Gjing
 **/
object DataSetSourceDemo {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
//    fromCollect(environment)
    fromFile(environment)
  }

  /**
   * 从集合读数据
   * @param env 环境
   */
  def fromCollect(env: ExecutionEnvironment): Unit = {
    val data = 1 to 10
    env.fromCollection(data).print()
  }

  /**
   * 从文件读数据
   * @param env 环境
   */
  def fromFile(env: ExecutionEnvironment): Unit = {
    env.readTextFile("C:\\Users\\me\\Desktop\\flink\\test.txt").print()
  }
}

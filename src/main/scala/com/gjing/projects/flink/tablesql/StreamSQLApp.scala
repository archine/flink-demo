package com.gjing.projects.flink.tablesql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * @author Gjing
 **/
object StreamSQLApp {
  def main(args: Array[String]): Unit = {
    // 创建基础运行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建table运行环境
    val tableEnvironment = StreamTableEnvironment.create(environment)
    val inputStream = environment.readTextFile("src\\main\\scala\\com\\gjing\\projects\\flink\\tablesql\\access.log")
    val mapStream = inputStream.map(x => {
      val splits = x.split(",")
      Access(splits(0).toLong, splits(1), splits(2).toInt)
    })
    val table = tableEnvironment.fromDataStream(mapStream)
    tableEnvironment.createTemporaryView("access", table)
    val result = tableEnvironment.sqlQuery("select * from access")
    tableEnvironment.toAppendStream[Row](result).print()
    environment.execute(this.getClass.getSimpleName)
  }
}

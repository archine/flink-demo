package com.gjing.projects.flink.tablesql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * @author Gjing
 **/
object StreamTableApp {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = StreamTableEnvironment.create(environment)
    val stream = environment.socketTextStream("127.0.0.1", 7777)
    // 数据以逗号分隔，切分后进行word count
    val stream2 = stream.flatMap(_.split(",").map(x => Wc(x.toLowerCase(), 1)))
    val table = tableEnvironment.fromDataStream(stream2)
    tableEnvironment.createTemporaryView("wc",table)
    val result = tableEnvironment.sqlQuery("select word,sum(num) from wc group by word")
    // table => stream
    tableEnvironment.toRetractStream[Row](result).print()
    environment.execute(this.getClass.getSimpleName)
  }
}

case class Wc(word:String,num:Int)

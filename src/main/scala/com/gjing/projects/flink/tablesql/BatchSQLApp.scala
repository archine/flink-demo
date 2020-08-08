package com.gjing.projects.flink.tablesql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * @author Gjing
 **/
object BatchSQLApp {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = BatchTableEnvironment.create(environment)
    val inputStream = environment.readTextFile("src\\main\\scala\\com\\gjing\\projects\\flink\\tablesql\\access.log")
    val mapStream = inputStream.map(x => {
      val splits = x.split(",")
      Access(splits(0).toLong, splits(1), splits(2).toInt)
    })
    val table = tableEnvironment.fromDataSet(mapStream)
    tableEnvironment.createTemporaryView("batch_access", table)
    val result = tableEnvironment.sqlQuery("select domain,num from batch_access")
    result.printSchema()
    tableEnvironment.toDataSet[Row](result).print()
  }
}

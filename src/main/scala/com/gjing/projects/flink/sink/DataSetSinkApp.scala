package com.gjing.projects.flink.sink

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * 写出到文件
 *
 * @author Gjing
 **/
object DataSetSinkApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filePath = "C:\\Users\\me\\Desktop\\flink\\"
    val data = 1 to (10)
    env.fromCollection(data).writeAsText(filePath, WriteMode.OVERWRITE)
    env.execute()
  }
}

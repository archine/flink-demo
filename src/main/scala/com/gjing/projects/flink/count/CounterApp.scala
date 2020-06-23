package com.gjing.projects.flink.count

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * 计数器
 *
 * @author Gjing
 **/
object CounterApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("张三", "李四")
    data.print()
    val number = data.map(new RichMapFunction[String, String] {
      //定义一个计数器
      val count = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("ele-counts-scala", count)
      }

      override def map(in: String): String = {
        count.add(1)
        in
      }
    }).setParallelism(2)
    number.print()
    number.writeAsText("C:\\Users\\me\\Desktop\\flink", WriteMode.OVERWRITE)
    val result = env.execute()
    println("sum: " + result.getAccumulatorResult[Long]("ele-counts-scala"))
  }
}

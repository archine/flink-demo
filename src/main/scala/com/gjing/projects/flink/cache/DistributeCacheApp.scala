package com.gjing.projects.flink.cache

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._

/**
 * @author Gjing
 **/
object DistributeCacheApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filePath = "file:\\C:\\Users\\me\\Desktop\\flink\\cache.txt"
    env.registerCachedFile(filePath,"pk-scala-dc")
    val data = env.fromElements("张三","李四")
    data.map(new RichMapFunction[String,String] {

      override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache.getFile("pk-scala-dc")
        val list = FileUtils.readLines(dcFile)
        //需要将list转为scala的
        for(s <- list.asScala){
          print(s)
        }
      }
      override def map(in: String): String = {
        in
      }
    }).print()
  }

}

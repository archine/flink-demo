package com.gjing.projects.flink.state

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author Gjing
 **/
object CheckPointApp {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.enableCheckpointing(5000)
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(5)))
    environment.setStateBackend(new FsStateBackend(""))
    val stream = environment.socketTextStream("127.0.0.1", 9999)
      .map(x => {
        if ("a".equals(x)) throw new RuntimeException("出错啦") else x
      })
      .print()
    environment.execute(this.getClass.getSimpleName)
  }
}

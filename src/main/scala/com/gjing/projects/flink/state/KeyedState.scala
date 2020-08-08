package com.gjing.projects.flink.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 使用KeyedState求平均数，输入的数据结构<k,v>
 * 每次到达两个元素就开始求平均数
 *
 * @author Gjing
 **/
object KeyedState {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.fromCollection(List((1, 3), (1, 5), (1, 7), (1, 2)))
      .keyBy(_._1)
      .flatMap(new RichFlatMapFunction[(Int, Int), (Int, Int)] {
        // 定义state
        private var state: ValueState[(Int, Int)] = _

        override def open(parameters: Configuration): Unit = {
          state = this.getRuntimeContext.getState(new ValueStateDescriptor[(Int, Int)]("average", createTypeInformation[(Int, Int)]))
        }

        override def flatMap(in: (Int, Int), collector: Collector[(Int, Int)]): Unit = {
          // 获取state
          val tmpState = state.value()
          val currentState = if (tmpState != null) tmpState else (0,0)
          // 更新
          val newState = (currentState._1 + 1, currentState._2 + in._2)
          state.update(newState)
          if (newState._1 >= 2) {
            collector.collect((in._1, newState._2 / newState._1))
            state.clear()
          }
        }
      }).print()

    environment.execute(this.getClass.getSimpleName)
  }

}

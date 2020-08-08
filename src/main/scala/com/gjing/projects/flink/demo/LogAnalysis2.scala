package com.gjing.projects.flink.demo

import java.util.Properties

import cn.gjing.tools.common.util.TimeUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * @author Gjing
 **/
object LogAnalysis2 {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 定义kafka主题
    val topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.20.9.1:9092")
    properties.setProperty("group.id", "test-gj")
    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    // 添加数据源
    val data = environment.addSource(consumer)
    data.map(x => {
      val splits = x.split("\t")
      val level = splits(2)
      val ip = splits(3)
      val traffic = splits(4).toLong
      val time = splits(5)
      var timeStamp = 0L
      try {
        timeStamp = TimeUtils.toDate2(time).getTime
      } catch {
        case e: Exception => logger.error(s"时间转换出错: $time", e.getMessage)
      }
      (level, ip, traffic, timeStamp)
    })
      // 过滤掉时间转换出错的
      .filter(_._4 != 0)
      // 只对level为e的进行处理
      .filter(_._1 == "E")
      // 对级别进行清洗，下面不在需要级别字段
      .map(x => {
        // 2: ip  3: traffic 4: time
        (x._2, x._3, x._4)
      })
      // 按照传递过来的时间进行时间戳排序
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long, Long)] {
        // 最大无序时间
        val maxOutOfOrderness = 3500L
        // 当前最大时间戳
        var currentMaxTimestamp: Long = _

        /**
         * 提取时间戳
         *
         * @param t 进来的元素
         * @param previousElementTimestamp 前一个元素的时间戳
         * @return 时间戳
         */
        override def extractTimestamp(t: (String, Long, Long), previousElementTimestamp: Long): Long = {
          val timestamp = t._3
          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
          timestamp
        }

        /**
         * 获取当前水印
         *
         * @return Watermark
         */
        override def getCurrentWatermark: Watermark = {
          new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        }
      })
      // 按照域名keyBy
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new WindowFunction[(String, Long, Long), (String, String, Long), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long, Long)], out: Collector[(String, String, Long)]): Unit = {
          // 由于上面有keyBy 因此可以通过tuple拿
          val ip = key.getField(0).toString
          var sum = 0L
          val iterator = input.iterator
          var minute = ""
          while (iterator.hasNext) {
            val next = iterator.next()
            // traffic求和
            sum += next._2
            if ("".equals(minute)) {
              minute = TimeUtils.toText(next._3, "yyyy-MM-dd HH:mm")
            }
          }
          // 第一个参数：时间   第二个参数 域名  第三个参数  traffic求和
          out.collect(minute, ip, sum)
        }
      })
      .print().setParallelism(1)

    environment.execute(this.getClass.getSimpleName)
  }
}

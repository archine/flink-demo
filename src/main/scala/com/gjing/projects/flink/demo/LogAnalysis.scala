package com.gjing.projects.flink.demo

import java.util.Properties

import cn.gjing.tools.common.util.TimeUtils
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * @author Gjing
 **/
object LogAnalysis {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.20.9.2:9092")
    properties.setProperty("group.id", "test-group")
    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    //接受kafka的数据
    val data = environment.addSource(consumer)
    val result = data.map(x => {
      val splits = x.split("\t")
      val name = splits(0)
      val en = splits(1)
      val str = splits(2)
      val time = splits(3).toLong
      (name, en, str, time)
    }).filter(!_._3.equals("c"))
      .map(e => {
        (e._2, e._3, e._4)
      })
      //添加水印
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, String, Long)] {
        val maxOutOfOrderness = 3500L
        var currentMaxTimestamp: Long = _
        override def getCurrentWatermark: Watermark = {
          new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        }
        override def extractTimestamp(t: (String, String, Long), l: Long): Long = {
          val timestamp = t._3
          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
          timestamp
        }
      }).keyBy(1)
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new WindowFunction[(String, String, Long), (String, String, Long), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, String, Long)], out: Collector[(String, String, Long)]): Unit = {
          val str = key.getField(0).toString
          var sum = 0L
          var time = "";
          val iterator = input.iterator
          while (iterator.hasNext) {
            val next = iterator.next()
            sum += 1
            time = TimeUtils.toText(next._3)
          }
          out.collect((time, str, sum))
        }
      })
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("172.20.9.2", 9200, "http"))
    val esSinkBuilder = new ElasticsearchSink.Builder[(String, String, Long)](
      httpHosts,
      new ElasticsearchSinkFunction[(String, String, Long)] {
        def process(element: (String, String, Long), ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element._1)
          json.put("str", element._2)
          json.put("sum", element._3)

          val rqst: IndexRequest = Requests.indexRequest
            .index("log-index")
            .`type`("log-demo")
            .source(json)
          indexer.add(rqst)
        }
      }
    )
    esSinkBuilder.setBulkFlushMaxActions(1)
    result.addSink(esSinkBuilder.build)
    environment.execute("LogAnalysis")
  }
}

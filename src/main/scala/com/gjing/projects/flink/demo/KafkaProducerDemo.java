package com.gjing.projects.flink.demo;

import cn.gjing.tools.common.util.RandomUtils;
import cn.gjing.tools.common.util.TimeUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.Properties;

/**
 * 模拟生产者发送数据
 * @author Gjing
 **/
public class KafkaProducerDemo {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.20.9.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "test";
        StringBuilder builder;
        while (true) {
            builder = new StringBuilder();
            builder.append("Gjing").append("\t")
                    .append("CN").append("\t")
                    // 事件等级
                    .append(getLevel()).append("\t")
                    // 模拟ip
                    .append(getIps()).append("\t")
                    // 模拟traffic
                    .append(RandomUtils.randomInt(10000)).append("\t")
                    // 时间
                    .append(TimeUtils.toText(LocalDateTime.now())).append("\t");
            String content = builder.toString();
            System.out.println(content);
            producer.send(new ProducerRecord<>(topic, content));
            Thread.sleep(2000);
        }
    }

    /**
     * 获取模拟ip
     * @return ip
     */
    public static String getIps() {
        String[] ips = new String[]{"123.123.123.11", "123.123.123.12", "123.123.123.13", "123.123.123.14", "172.20.9.1"};
        return ips[RandomUtils.randomInt(ips.length)];
    }

    /**
     * 获取等级
     * @return level
     */
    public static String getLevel() {
        String[] level = new String[]{"A", "B", "C", "D", "E"};
        return level[RandomUtils.randomInt(level.length)];
    }
}

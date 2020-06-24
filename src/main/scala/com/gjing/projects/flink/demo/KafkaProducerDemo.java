package com.gjing.projects.flink.demo;

import cn.gjing.tools.common.util.RandomUtils;
import cn.gjing.tools.common.util.TimeUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;

/**
 * @author Gjing
 **/
public class KafkaProducerDemo {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.20.9.2:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "test";
        StringBuilder builder;
        while (true) {
            builder = new StringBuilder();
            builder.append("Gjing").append("\t")
                    .append("CN").append("\t")
                    .append(RandomUtils.generateString(1)).append("\t")
                    .append(TimeUtils.toTimestamp(LocalDateTime.now())).append("\t");
            String content = builder.toString();
            System.out.println(content);
            producer.send(new ProducerRecord<>(topic, content));
            Thread.sleep(2000);
        }
    }
}

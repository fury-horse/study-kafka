package com.liangjun.study.studykafka.basic;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
* @desc    
* @version 1.0
* @author  Liang Jun
* @date    2020年04月14日 13:42:07
**/
public class KafkaConsumer4Assign {
    public static void main(String[] args) {
        //1.创建Kafka链接参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"HOST01:9092,HOST02:9092,HOST03:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //props.put(ConsumerConfig.GROUP_ID_CONFIG,"group01");

        //2.创建topic消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> topic01 = Arrays.asList(new TopicPartition("topic01", 0));
        //3.绑定topic并绑定分区
        consumer.assign(topic01);
        //4.或指定topic的partition的offset
        //consumer.seekToBeginning(topic01);
        //4.或指定topic的patition的offset
        consumer.seek(new TopicPartition("topic01", 0), 6);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));

            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, String> next = iterator.next();
                System.out.println(
                        "key: " + next.key() +
                        "\tvalue: " + next.value() +
                        "\toffset: " + next.offset() +
                        "\tpatition: " + next.partition()
                );
            }
        }

        //关闭连接
        //consumer.close();
    }
}
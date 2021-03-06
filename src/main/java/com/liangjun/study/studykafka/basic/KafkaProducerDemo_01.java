package com.liangjun.study.studykafka.basic;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
* @desc    生产者demo
* @version 1.0
* @author  Liang Jun
* @date    2020年04月14日 14:12:21
**/
public class KafkaProducerDemo_01 {
    public static void main(String[] args) {
        //1.创建连接参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"HOST01:9092,HOST02:9092,HOST03:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,UserDefineProducerInterceptor.class.getName());
        //props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,1);
        //props.put(ProducerConfig.ACKS_CONFIG,"-1");
        //props.put(ProducerConfig.RETRIES_CONFIG,10);

        //2.创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //3.发送消息
        for (int i=0; i<10; i++) {
            //若指定patition则会把所有消息发送到指定分区
            //ProducerRecord<String, String> record = new ProducerRecord<>("topic01", 1, "key" + i, "val" + i);
            //若指定key会根据key取hash值再对partition取模发送
            //ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "key" + i, "val" + i);
            //不指定key的情况，消息会轮询发到每个partition
            ProducerRecord<String, String> record = new ProducerRecord<>("topic01","val" + i);
            producer.send(record);
        }

        //关闭连接
        producer.close();
    }
}
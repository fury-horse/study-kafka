package com.liangjun.study.studykafka.serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {
    public static void main(String[] args) {
        //1.创建链接参数
        Properties props=new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"HOST01:9092,HOST02:9092,HOST03:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ObjectSerializer.class.getName());

        //2.创建生产者
        KafkaProducer<String,User> producer=new KafkaProducer<String, User>(props);

        //3.封账消息队列
        for(Integer i=0;i< 10;i++){
            ProducerRecord<String, User> record = new ProducerRecord<>("topic01", "key"+i,new User("username" + i, "password" + i));
            producer.send(record);
        }

        producer.close();
    }
}

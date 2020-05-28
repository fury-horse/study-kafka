package com.liangjun.study.studykafka.transaction;

import com.liangjun.study.studykafka.Constans;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * 生产端发送消息
 */
public class KafkaProducerDemo_01 {
    public static void main(String[] args) {
        //1.创建链接参数
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constans.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //事务ID,必须唯一
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id" + UUID.randomUUID().toString());

//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
//        props.put(ProducerConfig.ACKS_CONFIG, "all");
//        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);

        //2.创建生产者
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);
        producer.initTransactions(); //初始化事务
        try {
            producer.beginTransaction(); //开启事务
            //3.封账消息队列
            for(Integer i=0;i<10;i++){
                if (i == 8) {
                    i = 10 / 0;
                }
                ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "key" + i, "value" + i);
                producer.send(record);
                producer.flush();
                System.out.println("消息已发送" + i);
            }
            producer.commitTransaction(); //提交事务
        } catch (Exception e) {
            System.out.println("出现错误了 " + e.getMessage());
            producer.abortTransaction(); //终止事务
        }

        producer.close();
    }
}

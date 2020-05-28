package com.liangjun.study.studykafka.transaction;

import com.liangjun.study.studykafka.Constans;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 事务：从topic1消费，再发送到topic2
 * 数据一致性
 */
public class KafkaProducerDemo_02 {
    public static void main(String[] args) {

        //创建消费者
        KafkaConsumer<String,String> consumer = buildConsumer("group01");
        //创建生产者
        KafkaProducer<String,String> producer = buildProducer();

        //订阅消息
        consumer.subscribe(Pattern.compile("^topic01"));
        //初始化事务
        producer.initTransactions();

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();

                //开启事务
                producer.beginTransaction();
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                while (recordIterator.hasNext()){
                    ConsumerRecord<String, String> record = recordIterator.next();
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();

                    System.out.println("topic:" +topic+ ",partition:" +partition+ ",offset:" +offset+ ",key:" +key+ ",value:" +value);
                    offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset+1));

                    //创建新的Record
                    ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>("topic02", key, value);
                    producer.send(newRecord);
                }
                //提交事务
                producer.sendOffsetsToTransaction(offsets, "group1");
                producer.commitTransaction();
            }
        } catch (Exception e) {
            e.printStackTrace();
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }

    private static KafkaProducer<String, String> buildProducer() {
        //1.创建链接参数
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constans.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //事务ID,必须唯一
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id" + UUID.randomUUID().toString());

        //2.创建生产者
        return new KafkaProducer<String, String>(props);
    }

    private static KafkaConsumer<String, String> buildConsumer(String groupId) {
        //1.创建Kafka链接参数
        Properties props=new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constans.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //只读已提交的消息
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        //设为手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        //2.创建Topic消费者
        return new KafkaConsumer<String, String>(props);
    }
}

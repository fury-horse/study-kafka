package com.liangjun.study.studykafka;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
* @desc     消息发送
* @version 1.0
* @author  Liang Jun
* @date    2020年04月19日 01:19:18
**/

@Service
public class OrderService {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Transactional
    public void sendMessage(String topic, String message) {
        kafkaTemplate.send(new ProducerRecord(topic, message));
    }
}
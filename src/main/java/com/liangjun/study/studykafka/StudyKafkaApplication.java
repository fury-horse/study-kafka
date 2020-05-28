package com.liangjun.study.studykafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;

import java.io.IOException;

@SpringBootApplication
public class StudyKafkaApplication {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(StudyKafkaApplication.class, args);
		System.in.read();
	}

	@SendTo("topic02")
    @KafkaListener(topics = {"topic01"})
    public String kafkaConsumer1(ConsumerRecord<String, String> record) {
        System.out.println("record:" + record);
        return record.value() + ":from auto";
    }
}

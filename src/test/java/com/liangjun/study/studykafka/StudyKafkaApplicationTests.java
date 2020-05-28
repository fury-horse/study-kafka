package com.liangjun.study.studykafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = {StudyKafkaApplication.class})
@RunWith(SpringRunner.class)
public class StudyKafkaApplicationTests {
    @Autowired
    private KafkaTemplate kafkaTemplate;
    @Autowired
    private OrderService orderService;

    /**
     * 不用事务的方式
     */
    @Test
    public void testProducer1() {
        kafkaTemplate.send(new ProducerRecord("topic01", "BBBB"));
    }

    /** 使用事务的方式1 */
    @Test
    public void testProducer2() {
        kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback() {
            @Override
            public Object doInOperations(KafkaOperations kafkaOperations) {
                kafkaTemplate.send(new ProducerRecord("topic01", "CCCC"));
                return null;
            }
        });
    }

    /**
     * 使用事务方式2
     */
    @Test
    public void testProducer3() {
        orderService.sendMessage("topic01", "DDDD");
    }
}

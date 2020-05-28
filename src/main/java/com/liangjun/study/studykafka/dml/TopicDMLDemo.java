package com.liangjun.study.studykafka.dml;


import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
* @desc    TOPIC管理
* @version 1.0
* @author  Liang Jun
* @date    2020年04月14日 12:54:18
**/
public class TopicDMLDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //kafka client
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "HOST01:9092,HOST02:9092,HOST03:9092");
        KafkaAdminClient adminClient = (KafkaAdminClient) KafkaAdminClient.create(props);

        //创建topic
        List<NewTopic> newTopics = Arrays.asList(new NewTopic("topic03", 2, (short) 3));
        //adminClient.createTopics(newTopics); //异步创建
        //adminClient.createTopics(newTopics).all().get(); //同步创建

        //删除topic
        List<String> delTopics = Arrays.asList("topic02");
        adminClient.deleteTopics(delTopics); //异步删除
        //adminClient.deleteTopics(delTopics).all().get(); //同步删除

        //列表topic
        KafkaFuture<Set<String>> names = adminClient.listTopics().names();
        for (String name : names.get()) {
            System.out.println("topic:" + name);
        }

        //topic详情
        DescribeTopicsResult describeTopics = adminClient.describeTopics(Arrays.asList("topic01"));
        Map<String, TopicDescription> descMap = describeTopics.all().get();

        System.out.println("----------------------------------------------");
        for (Map.Entry<String, TopicDescription> descEntry : descMap.entrySet()) {
            System.out.println(descEntry.getKey() + ":" + descEntry.getValue());
        }
        System.out.println("----------------------------------------------");

        adminClient.close();
    }
}
package com.liangjun.study.studykafka.partitioner;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
* @desc    自定义分区实现类
* @version 1.0
* @author  Liang Jun
* @date    2020年04月14日 20:59:50
**/
public class MyPartitioner implements Partitioner {
    AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionsForTopic(topic).size();

        if (keyBytes == null || keyBytes.length == 0) {
            return counter.getAndIncrement() & Integer.MAX_VALUE & numPartitions;
        }

        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {
        System.out.println("close");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("configure");
    }
}
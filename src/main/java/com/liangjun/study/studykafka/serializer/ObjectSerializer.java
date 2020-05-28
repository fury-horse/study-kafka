package com.liangjun.study.studykafka.serializer;


import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
* @desc    
* @version 1.0
* @author  Liang Jun
* @date    2020年04月15日 11:08:10
**/
public class ObjectSerializer implements Serializer {
    @Override
    public void configure(Map configs, boolean isKey) {
        System.out.println("configure");
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return SerializationUtils.serialize((Serializable) data);
    }

    @Override
    public void close() {
        System.out.println("close");
    }
}
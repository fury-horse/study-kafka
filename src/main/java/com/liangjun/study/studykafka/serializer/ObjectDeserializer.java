package com.liangjun.study.studykafka.serializer;


import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
* @desc    反序列化
* @version 1.0
* @author  Liang Jun
* @date    2020年04月15日 11:15:37
**/
public class ObjectDeserializer implements Deserializer {
    @Override
    public void configure(Map configs, boolean isKey) {
        System.out.println("configure");
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return SerializationUtils.deserialize(data);
    }

    @Override
    public void close() {
        System.out.println("close");
    }
}
package com.kafka.study.kafka.stream.service;

import com.kafka.study.producer.client.dto.Message;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KStreamProcessor {

    @Value("${kafka.topic.produce1.name}")
    private String outputTopic1Name;

    public void process(KStream<String, Message> recordStream){
        recordStream
                .filter((key, value) -> value != null && !value.getState().equals("Florida"))
                .map((key, value) -> {
                    if (value.getState().equals("Ohio")) {
                        value.setPrice(0);
                    }
                    return KeyValue.pair(key, value);
                })
                .to(outputTopic1Name);
    }
}

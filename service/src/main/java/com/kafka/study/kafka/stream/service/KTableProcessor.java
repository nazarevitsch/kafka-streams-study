package com.kafka.study.kafka.stream.service;

import com.kafka.study.producer.client.dto.Message;
import com.kafka.study.stream.client.dto.TotalCount;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KTableProcessor {

    @Value("${kafka.topic.produce2.name}")
    private String outputTopic2Name;

    public void process(KStream<String, Message> stream, Serde<TotalCount> totalCountSerde){
        KGroupedStream<String, Integer> priceByBrand = stream
                .map((key, value) -> new KeyValue<>(value.getBrand(), value.getPrice()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()));

        final KTable<String, TotalCount> countAndSum = priceByBrand
                .aggregate(
                        TotalCount::new,
                        (key, value, aggregate) -> {
                            aggregate.setCount(aggregate.getCount() + 1);
                            aggregate.setAmount(aggregate.getAmount() + value);
                            return aggregate;
                            },
                        Materialized.with(Serdes.String(), totalCountSerde)
                );

        final KTable<String, Integer> sumPrices = countAndSum
                .mapValues(TotalCount::getAmount, Materialized.with(Serdes.String(), Serdes.Integer()));
//                        , Materialized.as("brand-total-price-amount"));

        sumPrices.toStream().to(outputTopic2Name, Produced.with(Serdes.String(), Serdes.Integer()));
    }
}

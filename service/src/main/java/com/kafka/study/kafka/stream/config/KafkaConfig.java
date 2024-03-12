package com.kafka.study.kafka.stream.config;

import com.kafka.study.kafka.stream.service.KStreamProcessor;
import com.kafka.study.kafka.stream.service.KTableProcessor;
import com.kafka.study.producer.client.dto.Message;
import com.kafka.study.stream.client.dto.TotalCount;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    @Value("${kafka.bootstrap.address}")
    private String bootstrapAddress;

    @Value("${kafka.topic.consume.name}")
    private String inputTopicName;

    @Value("${kafka.topic.produce1.name}")
    private String outputTopic1Name;

    @Value("${kafka.topic.produce2.name}")
    private String outputTopic2Name;

    @Value("${kafka.topic.produce1.partition}")
    private Integer partition1;

    @Value("${kafka.topic.produce2.partition}")
    private Integer partition2;

    @Value("${kafka.schema.registry.url}")
    private String schemaRegistryUrl;

    @Autowired
    private KStreamProcessor kStreamProcessor;

    @Autowired
    private KTableProcessor kTableProcessor;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> configProperties = new HashMap<>();
        configProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app");
        configProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        configProperties.put("schema.registry.url", schemaRegistryUrl);
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaStreamsConfiguration(configProperties);
    }

    @Bean
    public KStream<String, Message> kStream(StreamsBuilder kStreamBuilder) {
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);
        Serde<Message> messageSerde = new SpecificAvroSerde<>();
        messageSerde.configure(serdeConfig, false);
        Serde<TotalCount> totalCountSerde = new SpecificAvroSerde<>();
        totalCountSerde.configure(serdeConfig, false);

        KStream<String, Message> stream = kStreamBuilder.stream(inputTopicName, Consumed.with(Serdes.String(), messageSerde));
        this.kStreamProcessor.process(stream);
        this.kTableProcessor.process(stream, totalCountSerde);

        return stream;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topic1() {
        return new NewTopic(outputTopic1Name, partition1, (short) 1);
    }

    @Bean
    public NewTopic topic2() {
        return new NewTopic(outputTopic2Name, partition2, (short) 1);
    }
}

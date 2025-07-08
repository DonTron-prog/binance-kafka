package com.crypto.config;

import com.crypto.model.Trade;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;
    
    @Value("${kafka.topics.raw-trades}")
    private String rawTradesTopic;
    
    @Value("${kafka.topics.filtered-trades-btcusdt}")
    private String filteredTradesBtcTopic;
    
    @Value("${kafka.topics.filtered-trades-ethusdt}")
    private String filteredTradesEthTopic;
    
    @Value("${kafka.topics.filtered-trades-bnbusdt}")
    private String filteredTradesBnbTopic;
    
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put("spring.json.trusted.packages", "com.crypto.model");
        
        return new KafkaStreamsConfiguration(props);
    }
    
    @Bean
    public NewTopic rawTradesTopic() {
        return TopicBuilder.name(rawTradesTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
    
    @Bean
    public NewTopic filteredTradesBtcTopic() {
        return TopicBuilder.name(filteredTradesBtcTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    
    @Bean
    public NewTopic filteredTradesEthTopic() {
        return TopicBuilder.name(filteredTradesEthTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    
    @Bean
    public NewTopic filteredTradesBnbTopic() {
        return TopicBuilder.name(filteredTradesBnbTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
    
    @Bean
    public JsonSerde<Trade> tradeSerde() {
        return new JsonSerde<>(Trade.class);
    }
    
    @Bean
    public org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory<>();
        
        // Configure the consumer factory
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "websocket-consumer-group");
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
            org.apache.kafka.common.serialization.StringDeserializer.class);
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
            org.springframework.kafka.support.serializer.JsonDeserializer.class);
        consumerProps.put(org.springframework.kafka.support.serializer.JsonDeserializer.TRUSTED_PACKAGES, "com.crypto.model,com.crypto.streams");
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        
        org.springframework.kafka.core.DefaultKafkaConsumerFactory<String, Object> consumerFactory = 
            new org.springframework.kafka.core.DefaultKafkaConsumerFactory<>(consumerProps);
        
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        
        return factory;
    }
}
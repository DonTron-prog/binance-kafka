package com.crypto.cli;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.crypto.model.Trade;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

public class RawTradeConsumer {
    private static final String TOPIC = "crypto-raw-trades";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "raw-trade-cli-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        System.out.println("Starting Raw Trade Consumer...");
        System.out.println("Connecting to Kafka at: " + BOOTSTRAP_SERVERS);
        System.out.println("Reading from topic: " + TOPIC);
        System.out.println("Press Ctrl+C to stop\n");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Trade trade = objectMapper.readValue(record.value(), Trade.class);
                        LocalDateTime timestamp = LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(trade.getTimestamp()), 
                            ZoneId.systemDefault()
                        );
                        
                        System.out.printf("[%s] %s - Price: %s, Quantity: %s, Buyer: %s%n",
                            timestamp.format(formatter),
                            trade.getSymbol(),
                            trade.getPrice(),
                            trade.getQuantity(),
                            trade.isBuyerMaker() ? "Maker" : "Taker"
                        );
                    } catch (Exception e) {
                        System.err.println("Error parsing trade: " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error in consumer: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
package com.crypto.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams application that processes cryptocurrency trade data from Binance.
 * Filters trades for specific symbols (BTCUSDT, ETHUSDT, BNBUSDT), aggregates them
 * in 20-second tumbling windows to find maximum trade prices, and routes results
 * to dedicated output topics per symbol.
 */
public class BinanceFilteringApp {
    private static final Logger logger = LoggerFactory.getLogger(BinanceFilteringApp.class);
    
    private static final String INPUT_TOPIC = "crypto-raw-trades";
    private static final String OUTPUT_TOPIC_BTC = "filtered-trades-btcusdt";
    private static final String OUTPUT_TOPIC_ETH = "filtered-trades-ethusdt";
    private static final String OUTPUT_TOPIC_BNB = "filtered-trades-bnbusdt";
    
    private static final String SYMBOL_BTC = "BTCUSDT";
    private static final String SYMBOL_ETH = "ETHUSDT";
    private static final String SYMBOL_BNB = "BNBUSDT";
    
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Represents a trade record from Binance.
     */
    public static class TradeRecord {
        private String symbol;
        private double price;
        private double quantity;
        private long timestamp;
        
        public TradeRecord() {}
        
        public TradeRecord(String symbol, double price, double quantity, long timestamp) {
            this.symbol = symbol;
            this.price = price;
            this.quantity = quantity;
            this.timestamp = timestamp;
        }
        
        public String getSymbol() { return symbol; }
        public void setSymbol(String symbol) { this.symbol = symbol; }
        public double getPrice() { return price; }
        public void setPrice(double price) { this.price = price; }
        public double getQuantity() { return quantity; }
        public void setQuantity(double quantity) { this.quantity = quantity; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }

    /**
     * Represents the aggregated result for a symbol within a time window.
     */
    public static class AggregatedTradeRecord {
        private String symbol;
        private double maxPrice;
        private long windowStart;
        private long windowEnd;
        
        public AggregatedTradeRecord() {}
        
        public AggregatedTradeRecord(String symbol, double maxPrice, long windowStart, long windowEnd) {
            this.symbol = symbol;
            this.maxPrice = maxPrice;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }
        
        public String getSymbol() { return symbol; }
        public void setSymbol(String symbol) { this.symbol = symbol; }
        public double getMaxPrice() { return maxPrice; }
        public void setMaxPrice(double maxPrice) { this.maxPrice = maxPrice; }
        public long getWindowStart() { return windowStart; }
        public void setWindowStart(long windowStart) { this.windowStart = windowStart; }
        public long getWindowEnd() { return windowEnd; }
        public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }
    }

    /**
     * Builds the Kafka Streams topology for the application.
     *
     * @param serdeConfig Configuration map for SerDes (schema registry URL, auth, etc.)
     * @return The built topology
     */
    public static Topology buildTopology(Map<String, String> serdeConfig) {
        StreamsBuilder builder = new StreamsBuilder();
        
        logger.info("Building Kafka Streams topology for Binance filtering and aggregation");
        
        // Create source stream from raw binance data topic
        KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC,
            Consumed.with(Serdes.String(), Serdes.String())
                .withName("binance-data-source"));
        
        // Parse JSON and filter for target symbols
        KStream<String, TradeRecord> parsedStream = sourceStream
            .mapValues(jsonValue -> {
                try {
                    JsonNode node = objectMapper.readTree(jsonValue);
                    return new TradeRecord(
                        node.get("symbol").asText(),
                        node.get("price").asDouble(),
                        node.get("quantity").asDouble(),
                        node.get("timestamp").asLong()
                    );
                } catch (Exception e) {
                    logger.warn("Failed to parse trade record: {}", jsonValue, e);
                    return null;
                }
            }, Named.as("parse-json-records"))
            .filter((key, value) -> value != null, Named.as("filter-null-records"))
            .filter((key, value) -> {
                String symbol = value.getSymbol();
                return SYMBOL_BTC.equals(symbol) || SYMBOL_ETH.equals(symbol) || SYMBOL_BNB.equals(symbol);
            }, Named.as("filter-target-symbols"));
        
        // Re-key by symbol for grouping
        KStream<String, TradeRecord> rekeyedStream = parsedStream
            .selectKey((key, value) -> value.getSymbol(), Named.as("rekey-by-symbol"));
        
        // Group by key (symbol) and apply windowed aggregation
        KTable<Windowed<String>, Double> aggregatedTable = rekeyedStream
            .groupByKey(Grouped.with(Serdes.String(), 
                org.apache.kafka.common.serialization.Serdes.serdeFrom(
                    new TradeRecordSerializer(), new TradeRecordDeserializer()))
                .withName("group-by-symbol"))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(20)))
            .aggregate(
                () -> 0.0,
                (key, value, aggregate) -> {
                    logger.debug("Aggregating trade for symbol: {}, price: {}, current max: {}", 
                        key, value.getPrice(), aggregate);
                    return Math.max(aggregate, value.getPrice());
                },
                Materialized.<String, Double, WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("max-price-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Double())
            );
        
        // Convert windowed table back to stream and map to output format
        KStream<Windowed<String>, AggregatedTradeRecord> aggregatedStream = aggregatedTable
            .toStream(Named.as("windowed-table-to-stream"))
            .mapValues((windowedKey, maxPrice) -> {
                org.apache.kafka.streams.kstream.Window window = windowedKey.window();
                return new AggregatedTradeRecord(
                    windowedKey.key(),
                    maxPrice,
                    window.start(),
                    window.end()
                );
            }, Named.as("map-to-aggregated-record"));
        
        // Branch the stream by symbol and route to appropriate output topics
        KStream<Windowed<String>, AggregatedTradeRecord>[] branches = aggregatedStream
            .branch(Named.as("split-by-symbol"),
                (windowedKey, value) -> SYMBOL_BTC.equals(windowedKey.key()), 
                (windowedKey, value) -> SYMBOL_ETH.equals(windowedKey.key()),
                (windowedKey, value) -> SYMBOL_BNB.equals(windowedKey.key()));
        
        // Route each branch to its dedicated output topic
        if (branches.length >= 1) {
            branches[0]
                .mapValues(record -> {
                    try {
                        return objectMapper.writeValueAsString(record);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to serialize BTC aggregated record", e);
                        return null;
                    }
                }, Named.as("serialize-btc-records"))
                .filter((key, value) -> value != null, Named.as("filter-null-btc-serialized"))
                .to(OUTPUT_TOPIC_BTC, Produced.with(
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofSeconds(20).toMillis()),
                    Serdes.String()
                ).withName("btc-output-sink"));
        }
        
        if (branches.length >= 2) {
            branches[1]
                .mapValues(record -> {
                    try {
                        return objectMapper.writeValueAsString(record);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to serialize ETH aggregated record", e);
                        return null;
                    }
                }, Named.as("serialize-eth-records"))
                .filter((key, value) -> value != null, Named.as("filter-null-eth-serialized"))
                .to(OUTPUT_TOPIC_ETH, Produced.with(
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofSeconds(20).toMillis()),
                    Serdes.String()
                ).withName("eth-output-sink"));
        }
        
        if (branches.length >= 3) {
            branches[2]
                .mapValues(record -> {
                    try {
                        return objectMapper.writeValueAsString(record);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to serialize BNB aggregated record", e);
                        return null;
                    }
                }, Named.as("serialize-bnb-records"))
                .filter((key, value) -> value != null, Named.as("filter-null-bnb-serialized"))
                .to(OUTPUT_TOPIC_BNB, Produced.with(
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofSeconds(20).toMillis()),
                    Serdes.String()
                ).withName("bnb-output-sink"));
        }
        
        return builder.build();
    }

    /**
     * Outputs the topology description without starting the Kafka Streams application.
     * Usage: java BinanceFilteringAggregation4App --describe-topology
     */
    public static void describeTopology() {
        try {
            // Create minimal serde configuration for topology building
            Map<String, String> serdeConfig = Map.of(
                "schema.registry.url", "mock://test",
                "basic.auth.credentials.source", "USER_INFO",
                "basic.auth.user.info", "mock:mock"
            );
            
            // Build topology
            Topology topology = buildTopology(serdeConfig);
            
            // Output topology description
            System.out.println("=== Kafka Streams Topology Description ===");
            System.out.println(topology.describe());
            System.out.println("===========================================");
            
        } catch (Exception e) {
            logger.error("Error describing topology", e);
            System.exit(1);
        }
    }

    /**
     * Custom serializer for TradeRecord objects.
     */
    private static class TradeRecordSerializer implements org.apache.kafka.common.serialization.Serializer<TradeRecord> {
        @Override
        public byte[] serialize(String topic, TradeRecord data) {
            if (data == null) return null;
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize TradeRecord", e);
                return null;
            }
        }
    }

    /**
     * Custom deserializer for TradeRecord objects.
     */
    private static class TradeRecordDeserializer implements org.apache.kafka.common.serialization.Deserializer<TradeRecord> {
        @Override
        public TradeRecord deserialize(String topic, byte[] data) {
            if (data == null) return null;
            try {
                return objectMapper.readValue(data, TradeRecord.class);
            } catch (IOException e) {
                logger.error("Failed to deserialize TradeRecord", e);
                return null;
            }
        }
    }

    /**
     * Loads configuration properties from the ccloud.properties file.
     *
     * @return Properties object with loaded configuration
     */
    private static Properties loadConfig() {
        Properties props = new Properties();
        
        try (FileInputStream input = new FileInputStream("ccloud.properties")) {
            props.load(input);
            logger.info("Successfully loaded configuration from ccloud.properties");
        } catch (IOException e) {
            logger.error("Failed to load ccloud.properties file", e);
            throw new RuntimeException("Configuration file not found or not readable", e);
        }
        
        return props;
    }

    /**
     * Creates and configures Kafka Streams properties.
     *
     * @param config Base configuration properties
     * @return Properties configured for Kafka Streams
     */
    private static Properties createStreamsProperties(Properties config) {
        Properties streamsProps = new Properties();
        
        // Copy all base configuration
        streamsProps.putAll(config);
        
        // Kafka Streams specific configuration
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "binance-filtering-aggregation-4-app");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("bootstrap.servers"));
        
        // Performance and reliability settings
        streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
        streamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        streamsProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        streamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        
        // Consumer settings for better handling of large messages
        streamsProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576);
        streamsProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
        streamsProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 20000);
        
        // Enable exactly-once semantics for financial data accuracy
        streamsProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        
        logger.info("Configured Kafka Streams with application.id: {}", 
            streamsProps.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        
        return streamsProps;
    }

    public static void main(String[] args) {
        // Check if user wants to only describe topology
        if (args.length > 0 && "--describe-topology".equals(args[0])) {
            describeTopology();
            return;
        }
        
        logger.info("Starting Binance Filtering and Aggregation Kafka Streams application");
        
        try {
            // Load configuration
            Properties config = loadConfig();
            
            // Create Kafka Streams properties
            Properties streamsProps = createStreamsProperties(config);
            
            // Create serde configuration for topology building
            Map<String, String> serdeConfig = Map.of(
                "schema.registry.url", config.getProperty("schema.registry.url", "http://localhost:8081"),
                "basic.auth.credentials.source", config.getProperty("basic.auth.credentials.source", "USER_INFO"),
                "basic.auth.user.info", config.getProperty("basic.auth.user.info", "")
            );
            
            // Build the topology
            Topology topology = buildTopology(serdeConfig);
            
            // Output topology description for debugging/documentation
            System.out.println("=== Kafka Streams Topology Description ===");
            System.out.println(topology.describe());
            System.out.println("===========================================");
            
            // Create and configure Kafka Streams
            KafkaStreams streams = new KafkaStreams(topology, streamsProps);
            
            // Set up shutdown hook for graceful termination
            final CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    logger.info("Received shutdown signal, closing Kafka Streams...");
                    streams.close();
                    latch.countDown();
                }
            });
            
            // Set exception handler
            streams.setUncaughtExceptionHandler(throwable -> {
                logger.error("Uncaught exception in stream thread", throwable);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });
            
            try {
                // Start the streams application
                logger.info("Starting Kafka Streams application...");
                streams.start();
                
                // Wait for termination signal
                latch.await();
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Application interrupted");
            } finally {
                streams.close();
                logger.info("Kafka Streams application has been shut down");
            }
            
        } catch (Exception e) {
            logger.error("Failed to start Kafka Streams application", e);
            System.exit(1);
        }
    }
}
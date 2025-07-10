package goodlabs.copilot.streams.binancefiltering2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams application that filters incoming cryptocurrency trade data
 * from a single topic by trading symbol and routes trades to dedicated output topics.
 * Specifically filters for BTCUSDT, ETHUSDT, and BNBUSDT symbols.
 */
public class BinanceFilteringApp {
    private static final Logger logger = LoggerFactory.getLogger(BinanceFilteringApp.class);
    
    // Topic names
    private static final String SOURCE_TOPIC = "raw_binance_data";
    private static final String BTCUSDT_SINK_TOPIC = "filtered-trades-btcusdt";
    private static final String ETHUSDT_SINK_TOPIC = "filtered-trades-ethusdt";
    private static final String BNBUSDT_SINK_TOPIC = "filtered-trades-bnbusdt";
    
    // Trading symbols to filter
    private static final String BTCUSDT_SYMBOL = "BTCUSDT";
    private static final String ETHUSDT_SYMBOL = "ETHUSDT";
    private static final String BNBUSDT_SYMBOL = "BNBUSDT";

    /**
     * Creates a default configuration for the topology test.
     * Used when no specific configuration is needed.
     * 
     * @return Properties object with default test configuration
     */
    public static Properties createDefaultTestConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    /**
     * Builds the Kafka Streams topology for the application.
     * 
     * @param serdeConfig Configuration map for SerDes (not used in this JSON-based implementation)
     * @return The built topology
     */
    public static Topology buildTopology(Map<String, String> serdeConfig) {
        StreamsBuilder builder = new StreamsBuilder();
        
        // Configure String SerDes for key and value (assuming JSON string values)
        final Serde<String> stringSerde = Serdes.String();
        
        logger.info("Building topology with source topic: {}", SOURCE_TOPIC);
        
        try {
            // Create source stream from raw binance data topic
            KStream<String, String> sourceStream = builder.stream(SOURCE_TOPIC);
            
            // Branch the stream based on trading symbol
            // Note: Since no Avro schemas are provided, we assume JSON string format
            // and use string contains check as a fallback approach
            Map<String, KStream<String, String>> branches = sourceStream.split(Named.as("symbol-filter-"))
                .branch((key, value) -> {
                    try {
                        // Check if the JSON string contains the BTCUSDT symbol
                        return value != null && value.contains("\"symbol\":\"" + BTCUSDT_SYMBOL + "\"");
                    } catch (Exception e) {
                        logger.warn("Error processing record for BTCUSDT filter: {}", e.getMessage());
                        return false;
                    }
                }, Branched.as("btcusdt"))
                .branch((key, value) -> {
                    try {
                        // Check if the JSON string contains the ETHUSDT symbol
                        return value != null && value.contains("\"symbol\":\"" + ETHUSDT_SYMBOL + "\"");
                    } catch (Exception e) {
                        logger.warn("Error processing record for ETHUSDT filter: {}", e.getMessage());
                        return false;
                    }
                }, Branched.as("ethusdt"))
                .branch((key, value) -> {
                    try {
                        // Check if the JSON string contains the BNBUSDT symbol
                        return value != null && value.contains("\"symbol\":\"" + BNBUSDT_SYMBOL + "\"");
                    } catch (Exception e) {
                        logger.warn("Error processing record for BNBUSDT filter: {}", e.getMessage());
                        return false;
                    }
                }, Branched.as("bnbusdt"));
            
            // Get the individual streams from the branch map
            KStream<String, String> btcusdtStream = branches.get("symbol-filter-btcusdt");
            KStream<String, String> ethusdtStream = branches.get("symbol-filter-ethusdt");
            KStream<String, String> bnbusdtStream = branches.get("symbol-filter-bnbusdt");
            
            // Route each branch to its respective output topic
            if (btcusdtStream != null) {
                btcusdtStream
                    .peek((key, value) -> logger.debug("Routing BTCUSDT trade with key: {}", key))
                    .to(BTCUSDT_SINK_TOPIC);
            }
            
            if (ethusdtStream != null) {
                ethusdtStream
                    .peek((key, value) -> logger.debug("Routing ETHUSDT trade with key: {}", key))
                    .to(ETHUSDT_SINK_TOPIC);
            }
            
            if (bnbusdtStream != null) {
                bnbusdtStream
                    .peek((key, value) -> logger.debug("Routing BNBUSDT trade with key: {}", key))
                    .to(BNBUSDT_SINK_TOPIC);
            }
            
            logger.info("Topology built successfully with branches for BTCUSDT, ETHUSDT, and BNBUSDT");
            
        } catch (Exception e) {
            logger.error("Error building topology", e);
            throw new RuntimeException("Failed to build topology", e);
        }
        
        return builder.build();
    }

    /**
     * Outputs the topology description without starting the Kafka Streams application.
     * Usage: java BinanceFilteringApp --describe-topology
     */
    public static void describeTopology() {
        try {
            logger.info("Describing topology without starting Kafka Streams");
            
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
            System.out.println("==========================================");
            
            logger.info("Topology description completed successfully");
            
        } catch (Exception e) {
            logger.error("Error describing topology", e);
            System.exit(1);
        }
    }

    /**
     * Loads Kafka configuration from the ccloud.properties file.
     * 
     * @return Properties object with Kafka configuration
     * @throws IOException if properties file cannot be read
     */
    private static Properties loadKafkaConfig() throws IOException {
        Properties props = new Properties();
        
        try (FileInputStream fis = new FileInputStream("ccloud.properties")) {
            props.load(fis);
            logger.info("Successfully loaded configuration from ccloud.properties");
        }
        
        // Resolve environment variables in property values
        Properties resolvedProps = new Properties();
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            resolvedProps.setProperty(key, resolveEnvironmentVariables(value));
        }
        
        return resolvedProps;
    }
    
    /**
     * Resolves environment variables in property values.
     * 
     * @param value The property value potentially containing ${ENV_VAR} placeholders
     * @return The resolved value with environment variables substituted
     */
    private static String resolveEnvironmentVariables(String value) {
        if (value == null) return null;
        
        String result = value;
        int start = result.indexOf("${");
        while (start != -1) {
            int end = result.indexOf("}", start);
            if (end != -1) {
                String envVar = result.substring(start + 2, end);
                String envValue = System.getenv(envVar);
                if (envValue != null) {
                    result = result.substring(0, start) + envValue + result.substring(end + 1);
                } else {
                    logger.warn("Environment variable '{}' is not set", envVar);
                }
            }
            start = result.indexOf("${", start + 1);
        }
        
        return result;
    }

    /**
     * Creates Kafka Streams configuration properties.
     * 
     * @param kafkaConfig Base Kafka configuration loaded from properties file
     * @return Complete StreamsConfig properties
     */
    private static Properties createStreamsConfig(Properties kafkaConfig) {
        Properties streamsProps = new Properties();
        
        // Copy all Kafka config properties
        streamsProps.putAll(kafkaConfig);
        
        // Add Kafka Streams specific configuration
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "binance-filtering-2-app");
        streamsProps.put(StreamsConfig.CLIENT_ID_CONFIG, "binance-filtering-2-client");
        
        // Set default SerDes
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // Performance and reliability settings
        streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L); // 10MB
        streamsProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        
        // Consumer configuration for better reliability
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        return streamsProps;
    }

    /**
     * Main method for the Binance Filtering Kafka Streams application.
     * 
     * @param args Command line arguments. Use "--describe-topology" to only output topology description.
     */
    public static void main(String[] args) {
        // Check if user wants to only describe topology
        if (args.length > 0 && "--describe-topology".equals(args[0])) {
            describeTopology();
            return;
        }
        
        logger.info("Starting Binance Filtering Kafka Streams Application");
        
        Properties kafkaConfig;
        try {
            kafkaConfig = loadKafkaConfig();
        } catch (IOException e) {
            logger.error("Failed to load Kafka configuration from ccloud.properties", e);
            System.exit(1);
            return;
        }
        
        // Create Streams configuration
        Properties streamsProps = createStreamsConfig(kafkaConfig);
        
        // Build the topology
        Topology topology;
        try {
            // Create serde configuration map (empty for this JSON-based application)
            Map<String, String> serdeConfig = Map.of();
            topology = buildTopology(serdeConfig);
            
            // Output topology description for debugging/documentation
            System.out.println("=== Kafka Streams Topology Description ===");
            System.out.println(topology.describe());
            System.out.println("==========================================");
            
        } catch (Exception e) {
            logger.error("Failed to build topology", e);
            System.exit(1);
            return;
        }
        
        // Create and configure the Kafka Streams instance
        final KafkaStreams streams = new KafkaStreams(topology, streamsProps);
        final CountDownLatch latch = new CountDownLatch(1);
        
        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread("binance-filtering-shutdown-hook") {
            @Override
            public void run() {
                logger.info("Shutting down Binance Filtering Streams Application");
                streams.close();
                latch.countDown();
            }
        });
        
        // Set up uncaught exception handler
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            logger.error("Uncaught exception in thread {}: {}", thread.getName(), throwable.getMessage(), throwable);
            streams.close();
            latch.countDown();
        });
        
        try {
            logger.info("Starting Kafka Streams with application ID: {}", streamsProps.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
            streams.start();
            logger.info("Kafka Streams application started successfully");
            
            // Wait for termination signal
            latch.await();
            
        } catch (Exception e) {
            logger.error("Error running Kafka Streams application", e);
            System.exit(1);
        } finally {
            streams.close();
            logger.info("Binance Filtering Kafka Streams Application stopped");
        }
    }
}
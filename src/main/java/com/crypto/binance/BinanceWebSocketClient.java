package com.crypto.binance;

import com.crypto.model.Trade;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class BinanceWebSocketClient {
    
    private final KafkaTemplate<String, Trade> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    @Value("${binance.websocket.base-url}")
    private String baseUrl;
    
    @Value("#{'${binance.websocket.symbols}'.split(',')}")
    private List<String> symbols;
    
    @Value("${kafka.topics.raw-trades}")
    private String rawTradesTopic;
    
    private WebSocketClient webSocketClient;
    private CountDownLatch connectionLatch = new CountDownLatch(1);
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean isReconnecting = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final AtomicLong lastMessageTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong messageCount = new AtomicLong(0);
    
    @PostConstruct
    public void connect() {
        String streamUrl = buildStreamUrl();
        log.info("Connecting to Binance WebSocket: {}", streamUrl);
        
        try {
            URI uri = new URI(streamUrl);
            webSocketClient = createWebSocketClient(uri);
            webSocketClient.connect();
            
            // Wait for connection to be established
            if (!connectionLatch.await(10, TimeUnit.SECONDS)) {
                log.error("Failed to connect to Binance WebSocket within timeout");
            }
        } catch (Exception e) {
            log.error("Error connecting to Binance WebSocket", e);
        }
    }
    
    private WebSocketClient createWebSocketClient(URI uri) {
        return new WebSocketClient(uri) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                log.info("Connected to Binance WebSocket");
                isConnected.set(true);
                isReconnecting.set(false);
                connectionLatch.countDown();
            }
            
            @Override
            public void onMessage(String message) {
                processMessage(message);
            }
            
            @Override
            public void onMessage(ByteBuffer bytes) {
                // Handle ping frames by responding with pong
                if (bytes != null && bytes.hasRemaining()) {
                    log.debug("Received ping, sending pong");
                    send(bytes.array());
                }
            }
            
            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.warn("WebSocket connection closed: {} - {} (remote: {})", code, reason, remote);
                isConnected.set(false);
                
                // Schedule reconnection in a separate thread to avoid IllegalStateException
                if (!isReconnecting.get()) {
                    scheduleReconnection();
                }
            }
            
            @Override
            public void onError(Exception ex) {
                log.error("WebSocket error", ex);
                isConnected.set(false);
            }
        };
    }
    
    private void processMessage(String message) {
        try {
            // Update message tracking
            lastMessageTime.set(System.currentTimeMillis());
            long currentCount = messageCount.incrementAndGet();
            
            // Log every message to debug data flow
            log.info("Received WebSocket message #{}: {}", currentCount, message);
            
            // Simple rate limiting check (5 messages per second as per Binance limits)
            if (currentCount % 10 == 0) {
                log.info("Processed {} messages so far", currentCount);
            }
            
            // For combined streams, the format is: {"stream":"<streamName>", "data":<rawPayload>}
            if (message.contains("\"stream\":")) {
                log.info("Processing combined stream message");
                // Parse the combined stream format
                var jsonNode = objectMapper.readTree(message);
                String streamName = jsonNode.get("stream").asText();
                var dataNode = jsonNode.get("data");
                
                log.info("Stream: {}, Data: {}", streamName, dataNode.toString());
                
                Trade trade = objectMapper.treeToValue(dataNode, Trade.class);
                processTrade(trade);
            } else {
                // Direct trade format
                log.info("Processing direct trade message");
                Trade trade = objectMapper.readValue(message, Trade.class);
                processTrade(trade);
            }
        } catch (Exception e) {
            log.error("Error processing WebSocket message: {}", message, e);
        }
    }
    
    private void processTrade(Trade trade) {
        log.info("Processing trade: {} - {} @ {} (eventType: {})", 
            trade.getSymbol(), trade.getQuantity(), trade.getPrice(), trade.getEventType());
            
        if ("trade".equals(trade.getEventType())) {
            kafkaTemplate.send(rawTradesTopic, trade.getSymbol(), trade)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send trade to Kafka", ex);
                    } else {
                        log.info("✅ Trade sent to Kafka: {} @ {}", 
                            trade.getSymbol(), trade.getPrice());
                    }
                });
        } else {
            log.warn("Skipping non-trade event: {}", trade.getEventType());
        }
    }
    
    private String buildStreamUrl() {
        StringBuilder url = new StringBuilder(baseUrl);
        url.append("/stream?streams=");
        
        for (int i = 0; i < symbols.size(); i++) {
            if (i > 0) {
                url.append("/");
            }
            url.append(symbols.get(i)).append("@trade");
        }
        
        return url.toString();
    }
    
    private void scheduleReconnection() {
        if (isReconnecting.compareAndSet(false, true)) {
            log.info("Scheduling reconnection to Binance WebSocket in 5 seconds...");
            scheduler.schedule(() -> {
                try {
                    log.info("Attempting to reconnect to Binance WebSocket...");
                    connectionLatch = new CountDownLatch(1);
                    connect();
                } catch (Exception e) {
                    log.error("Reconnection failed", e);
                    isReconnecting.set(false);
                    // Retry again after another delay
                    scheduler.schedule(this::scheduleReconnection, 10, TimeUnit.SECONDS);
                }
            }, 5, TimeUnit.SECONDS);
        }
    }
    
    public boolean isConnected() {
        return isConnected.get() && webSocketClient != null && webSocketClient.isOpen();
    }
    
    public long getMessageCount() {
        return messageCount.get();
    }
    
    public long getLastMessageTime() {
        return lastMessageTime.get();
    }
    
    public boolean isHealthy() {
        long timeSinceLastMessage = System.currentTimeMillis() - lastMessageTime.get();
        return isConnected() && timeSinceLastMessage < 60000; // Healthy if message received within last 60 seconds
    }
    
    @PreDestroy
    public void disconnect() {
        log.info("Shutting down Binance WebSocket client");
        isConnected.set(false);
        isReconnecting.set(true); // Prevent reconnection attempts
        
        if (webSocketClient != null && webSocketClient.isOpen()) {
            log.info("Closing Binance WebSocket connection");
            webSocketClient.close();
        }
        
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
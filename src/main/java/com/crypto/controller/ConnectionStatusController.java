package com.crypto.controller;

import com.crypto.binance.BinanceWebSocketClient;
import com.crypto.model.Trade;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class ConnectionStatusController {
    
    private final BinanceWebSocketClient binanceWebSocketClient;
    private final SimpMessagingTemplate messagingTemplate;
    
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getConnectionStatus() {
        Map<String, Object> status = new HashMap<>();
        
        boolean connected = binanceWebSocketClient.isConnected();
        boolean healthy = binanceWebSocketClient.isHealthy();
        
        status.put("connected", connected);
        status.put("healthy", healthy);
        status.put("messageCount", binanceWebSocketClient.getMessageCount());
        status.put("lastMessageTime", Instant.ofEpochMilli(binanceWebSocketClient.getLastMessageTime()).toString());
        status.put("timestamp", Instant.now().toString());
        
        if (connected && healthy) {
            return ResponseEntity.ok(status);
        } else {
            return ResponseEntity.status(503).body(status); // Service Unavailable
        }
    }
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> getHealth() {
        Map<String, String> health = new HashMap<>();
        
        if (binanceWebSocketClient.isHealthy()) {
            health.put("status", "UP");
            health.put("binanceConnection", "CONNECTED");
            return ResponseEntity.ok(health);
        } else {
            health.put("status", "DOWN");
            health.put("binanceConnection", "DISCONNECTED");
            return ResponseEntity.status(503).body(health);
        }
    }
    
    @GetMapping("/test-data")
    public ResponseEntity<String> sendTestData() {
        // Create a test trade with random price variation
        Trade testTrade = new Trade();
        testTrade.setEventType("trade");
        testTrade.setSymbol("BTCUSDT");
        
        // Random price between $95,000 and $105,000
        double randomPrice = 95000 + (Math.random() * 10000);
        testTrade.setPrice(new BigDecimal(String.format("%.2f", randomPrice)));
        testTrade.setQuantity(new BigDecimal("0.001"));
        testTrade.setTradeId(System.currentTimeMillis());
        testTrade.setTradeTime(System.currentTimeMillis());
        testTrade.setIsBuyerMaker(false);
        
        // Send directly to WebSocket to test frontend connection
        messagingTemplate.convertAndSend("/topic/trades/BTCUSDT", testTrade);
        
        return ResponseEntity.ok("Test trade data sent: BTCUSDT @ $" + randomPrice);
    }
    
    @GetMapping("/test-volume")
    public ResponseEntity<String> sendTestVolumeData() {
        // Create test volume data
        var volumeData = new java.util.HashMap<String, Object>();
        volumeData.put("symbol", "BTCUSDT");
        volumeData.put("buyVolume", Math.random() * 1000000);
        volumeData.put("sellVolume", Math.random() * 1000000);
        volumeData.put("windowStart", System.currentTimeMillis() - 60000);
        volumeData.put("windowEnd", System.currentTimeMillis());
        
        // Send to volume topic
        messagingTemplate.convertAndSend("/topic/volume/BTCUSDT", volumeData);
        
        return ResponseEntity.ok("Test volume data sent: BTCUSDT");
    }
}
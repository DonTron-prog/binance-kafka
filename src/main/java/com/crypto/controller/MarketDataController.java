package com.crypto.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class MarketDataController {
    
    @GetMapping("/api/symbols")
    public List<String> getAvailableSymbols() {
        return List.of("BTCUSDT", "ETHUSDT", "BNBUSDT");
    }
    
    @GetMapping("/api/streams")
    public Map<String, List<String>> getAvailableStreams() {
        return Map.of(
            "prices", List.of("1m", "5m", "15m"),
            "trends", List.of("current"),
            "volume", List.of("1m", "5m", "1h"),
            "alerts", List.of("all", "symbol-specific")
        );
    }
    
    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "service", "binance-kafka-streams");
    }
}

@Controller
@Slf4j
class WebSocketController {
    
    @MessageMapping("/subscribe")
    @SendTo("/topic/subscription")
    public String handleSubscription(String symbol) {
        log.info("New subscription for symbol: {}", symbol);
        return "Subscribed to " + symbol;
    }
}
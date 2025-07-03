package com.crypto.service;

import com.crypto.model.Trade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class BinanceDataService {
    
    private final KafkaTemplate<String, Trade> kafkaTemplate;
    
    public CompletableFuture<Boolean> publishTrade(Trade trade, String topic) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                kafkaTemplate.send(topic, trade.getSymbol(), trade).get();
                log.debug("Published trade for {} at price {}", trade.getSymbol(), trade.getPrice());
                return true;
            } catch (Exception e) {
                log.error("Failed to publish trade for {}", trade.getSymbol(), e);
                return false;
            }
        });
    }
}
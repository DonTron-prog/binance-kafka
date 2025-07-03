package com.crypto.websocket;

import com.crypto.model.AggregatedPrice;
import com.crypto.model.PriceTrend;
import com.crypto.model.Trade;
import com.crypto.streams.AlertStream.Alert;
import com.crypto.streams.VolumeAnalyticsStream.VolumeAnalytics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class MarketDataPublisher {
    
    private final SimpMessagingTemplate messagingTemplate;
    
    @KafkaListener(
        topics = "${kafka.topics.aggregated-prices}",
        groupId = "websocket-price-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void publishAggregatedPrices(AggregatedPrice price) {
        log.info("🔔 Received aggregated price from Kafka: {} @ {}", price.getSymbol(), price.getClose());
        String destination = "/topic/prices/" + price.getSymbol();
        messagingTemplate.convertAndSend(destination, price);
        log.info("📡 Published price update for {} to WebSocket topic: {}", price.getSymbol(), destination);
    }
    
    @KafkaListener(
        topics = "${kafka.topics.price-trends}",
        groupId = "websocket-trend-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void publishPriceTrends(PriceTrend trend) {
        String destination = "/topic/trends/" + trend.getSymbol();
        messagingTemplate.convertAndSend(destination, trend);
        log.debug("Published trend update for {} to WebSocket", trend.getSymbol());
    }
    
    @KafkaListener(
        topics = "${kafka.topics.volume-analytics}",
        groupId = "websocket-volume-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void publishVolumeAnalytics(VolumeAnalytics volume) {
        String destination = "/topic/volume/" + volume.getSymbol();
        messagingTemplate.convertAndSend(destination, volume);
        log.debug("Published volume analytics for {} to WebSocket", volume.getSymbol());
    }
    
    @KafkaListener(
        topics = "${kafka.topics.alerts}",
        groupId = "websocket-alert-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void publishAlerts(Alert alert) {
        // Broadcast to general alerts topic
        messagingTemplate.convertAndSend("/topic/alerts", alert);
        
        // Also send to symbol-specific alert topic
        String symbolDestination = "/topic/alerts/" + alert.getSymbol();
        messagingTemplate.convertAndSend(symbolDestination, alert);
        
        log.info("Published alert: {} - {}", alert.getAlertType(), alert.getMessage());
    }
    
    // Temporary listener for raw trades to test data flow
    @KafkaListener(
        topics = "${kafka.topics.raw-trades}",
        groupId = "websocket-raw-trades-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void publishRawTrades(Trade trade) {
        log.info("🔥 Received RAW TRADE from Kafka: {} - {} @ {} (ID: {})", 
            trade.getSymbol(), trade.getQuantity(), trade.getPrice(), trade.getTradeId());
        
        // Send raw trade data to frontend for immediate testing
        String destination = "/topic/trades/" + trade.getSymbol();
        messagingTemplate.convertAndSend(destination, trade);
        log.info("📡 Published raw trade for {} to WebSocket topic: {}", trade.getSymbol(), destination);
    }
}
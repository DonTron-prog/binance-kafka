package com.crypto.websocket;

import com.crypto.model.Trade;
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
        topics = "${kafka.topics.filtered-trades-btcusdt}",
        groupId = "websocket-btc-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void publishBtcTrades(Trade trade) {
        log.debug("Publishing BTC trade: {} @ {}", trade.getQuantity(), trade.getPrice());
        messagingTemplate.convertAndSend("/topic/filtered-trades/BTCUSDT", trade);
    }
    
    @KafkaListener(
        topics = "${kafka.topics.filtered-trades-ethusdt}",
        groupId = "websocket-eth-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void publishEthTrades(Trade trade) {
        log.debug("Publishing ETH trade: {} @ {}", trade.getQuantity(), trade.getPrice());
        messagingTemplate.convertAndSend("/topic/filtered-trades/ETHUSDT", trade);
    }
    
    @KafkaListener(
        topics = "${kafka.topics.filtered-trades-bnbusdt}",
        groupId = "websocket-bnb-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void publishBnbTrades(Trade trade) {
        log.debug("Publishing BNB trade: {} @ {}", trade.getQuantity(), trade.getPrice());
        messagingTemplate.convertAndSend("/topic/filtered-trades/BNBUSDT", trade);
    }
}
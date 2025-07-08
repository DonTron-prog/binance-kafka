package com.crypto.websocket;

import com.crypto.dto.TradeDTO;
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
        log.info("Received BTC trade: {}", trade);
        log.info("BTC trade details - Price: {}, Quantity: {}, Symbol: {}, TradeTime: {}", 
            trade.getPrice(), trade.getQuantity(), trade.getSymbol(), trade.getTradeTime());
        log.debug("Publishing BTC trade: {} @ {}", trade.getQuantity(), trade.getPrice());
        
        TradeDTO dto = TradeDTO.builder()
            .symbol(trade.getSymbol())
            .price(trade.getPrice())
            .quantity(trade.getQuantity())
            .volume(trade.getVolume())
            .tradeTime(trade.getTradeTime())
            .isBuyerMaker(trade.getIsBuyerMaker())
            .eventType(trade.getEventType())
            .eventTime(trade.getEventTime())
            .tradeId(trade.getTradeId())
            .build();
            
        messagingTemplate.convertAndSend("/topic/filtered-trades/BTCUSDT", dto);
    }
    
    @KafkaListener(
        topics = "${kafka.topics.filtered-trades-ethusdt}",
        groupId = "websocket-eth-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void publishEthTrades(Trade trade) {
        log.debug("Publishing ETH trade: {} @ {}", trade.getQuantity(), trade.getPrice());
        
        TradeDTO dto = TradeDTO.builder()
            .symbol(trade.getSymbol())
            .price(trade.getPrice())
            .quantity(trade.getQuantity())
            .volume(trade.getVolume())
            .tradeTime(trade.getTradeTime())
            .isBuyerMaker(trade.getIsBuyerMaker())
            .eventType(trade.getEventType())
            .eventTime(trade.getEventTime())
            .tradeId(trade.getTradeId())
            .build();
            
        messagingTemplate.convertAndSend("/topic/filtered-trades/ETHUSDT", dto);
    }
    
    @KafkaListener(
        topics = "${kafka.topics.filtered-trades-bnbusdt}",
        groupId = "websocket-bnb-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void publishBnbTrades(Trade trade) {
        log.debug("Publishing BNB trade: {} @ {}", trade.getQuantity(), trade.getPrice());
        
        TradeDTO dto = TradeDTO.builder()
            .symbol(trade.getSymbol())
            .price(trade.getPrice())
            .quantity(trade.getQuantity())
            .volume(trade.getVolume())
            .tradeTime(trade.getTradeTime())
            .isBuyerMaker(trade.getIsBuyerMaker())
            .eventType(trade.getEventType())
            .eventTime(trade.getEventTime())
            .tradeId(trade.getTradeId())
            .build();
            
        messagingTemplate.convertAndSend("/topic/filtered-trades/BNBUSDT", dto);
    }
}
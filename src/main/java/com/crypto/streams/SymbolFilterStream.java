package com.crypto.streams;

import com.crypto.model.Trade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SymbolFilterStream {
    
    @Value("${kafka.topics.raw-trades}")
    private String rawTradesTopic;
    
    @Value("${kafka.topics.filtered-trades-btcusdt}")
    private String filteredTradesBtcTopic;
    
    @Value("${kafka.topics.filtered-trades-ethusdt}")
    private String filteredTradesEthTopic;
    
    @Value("${kafka.topics.filtered-trades-bnbusdt}")
    private String filteredTradesBnbTopic;
    
    private final JsonSerde<Trade> tradeSerde;
    
    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, Trade> tradesStream = streamsBuilder.stream(
            rawTradesTopic,
            Consumed.with(Serdes.String(), tradeSerde)
        );
        
        // Filter for BTC/USDT trades
        tradesStream
            .filter((key, trade) -> "BTCUSDT".equals(trade.getSymbol()))
            .to(filteredTradesBtcTopic, Produced.with(Serdes.String(), tradeSerde));
        
        // Filter for ETH/USDT trades
        tradesStream
            .filter((key, trade) -> "ETHUSDT".equals(trade.getSymbol()))
            .to(filteredTradesEthTopic, Produced.with(Serdes.String(), tradeSerde));
        
        // Filter for BNB/USDT trades
        tradesStream
            .filter((key, trade) -> "BNBUSDT".equals(trade.getSymbol()))
            .to(filteredTradesBnbTopic, Produced.with(Serdes.String(), tradeSerde));
        
        log.info("Symbol filter stream pipeline configured");
    }
}
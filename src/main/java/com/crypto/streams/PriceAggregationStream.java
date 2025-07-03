package com.crypto.streams;

import com.crypto.model.AggregatedPrice;
import com.crypto.model.Trade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class PriceAggregationStream {
    
    @Value("${kafka.topics.raw-trades}")
    private String rawTradesTopic;
    
    @Value("${kafka.topics.aggregated-prices}")
    private String aggregatedPricesTopic;
    
    private final JsonSerde<Trade> tradeSerde;
    private final JsonSerde<AggregatedPrice> aggregatedPriceSerde;
    
    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, Trade> tradesStream = streamsBuilder.stream(
            rawTradesTopic,
            Consumed.with(Serdes.String(), tradeSerde)
        );
        
        // 1-minute aggregations
        createAggregations(tradesStream, Duration.ofMinutes(1), "1m");
        
        // 5-minute aggregations
        createAggregations(tradesStream, Duration.ofMinutes(5), "5m");
        
        // 15-minute aggregations
        createAggregations(tradesStream, Duration.ofMinutes(15), "15m");
    }
    
    private void createAggregations(KStream<String, Trade> tradesStream, 
                                    Duration windowSize, 
                                    String windowName) {
        
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);
        
        KTable<Windowed<String>, AggregatedPrice> aggregated = tradesStream
            .groupByKey(Grouped.with(Serdes.String(), tradeSerde))
            .windowedBy(timeWindows)
            .aggregate(
                this::initializeAggregation,
                this::aggregateTrade,
                Materialized.with(Serdes.String(), aggregatedPriceSerde)
            );
        
        aggregated.toStream()
            .map((windowedKey, aggregatedPrice) -> {
                aggregatedPrice.setWindowStart(
                    Instant.ofEpochMilli(windowedKey.window().start())
                );
                aggregatedPrice.setWindowEnd(
                    Instant.ofEpochMilli(windowedKey.window().end())
                );
                return KeyValue.pair(
                    windowedKey.key() + "-" + windowName,
                    aggregatedPrice
                );
            })
            .to(aggregatedPricesTopic, 
                Produced.with(Serdes.String(), aggregatedPriceSerde));
    }
    
    private AggregatedPrice initializeAggregation() {
        return AggregatedPrice.builder()
            .open(BigDecimal.ZERO)
            .high(BigDecimal.ZERO)
            .low(new BigDecimal("999999999"))
            .close(BigDecimal.ZERO)
            .volume(BigDecimal.ZERO)
            .tradeCount(0L)
            .vwap(BigDecimal.ZERO)
            .build();
    }
    
    private AggregatedPrice aggregateTrade(String key, Trade trade, AggregatedPrice agg) {
        BigDecimal price = trade.getPrice();
        BigDecimal volume = trade.getVolume();
        
        // Set symbol
        agg.setSymbol(trade.getSymbol());
        
        // Update OHLC
        if (agg.getOpen().compareTo(BigDecimal.ZERO) == 0) {
            agg.setOpen(price);
        }
        agg.setClose(price);
        agg.setHigh(agg.getHigh().max(price));
        agg.setLow(agg.getLow().min(price));
        
        // Update volume and trade count
        agg.setVolume(agg.getVolume().add(volume));
        agg.setTradeCount(agg.getTradeCount() + 1);
        
        // Calculate VWAP
        BigDecimal totalValue = agg.getVwap()
            .multiply(new BigDecimal(agg.getTradeCount() - 1))
            .add(volume);
        agg.setVwap(totalValue.divide(
            new BigDecimal(agg.getTradeCount()), 
            8, 
            RoundingMode.HALF_UP
        ));
        
        return agg;
    }
}
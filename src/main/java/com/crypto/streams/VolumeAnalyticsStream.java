package com.crypto.streams;

import com.crypto.model.Trade;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
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
public class VolumeAnalyticsStream {
    
    @Value("${kafka.topics.raw-trades}")
    private String rawTradesTopic;
    
    @Value("${kafka.topics.volume-analytics}")
    private String volumeAnalyticsTopic;
    
    private final JsonSerde<Trade> tradeSerde;
    
    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, Trade> tradesStream = streamsBuilder.stream(
            rawTradesTopic,
            Consumed.with(Serdes.String(), tradeSerde)
        );
        
        // Calculate volume analytics for different time windows
        createVolumeAnalytics(tradesStream, Duration.ofMinutes(1), "1m");
        createVolumeAnalytics(tradesStream, Duration.ofMinutes(5), "5m");
        createVolumeAnalytics(tradesStream, Duration.ofHours(1), "1h");
    }
    
    private void createVolumeAnalytics(KStream<String, Trade> tradesStream,
                                       Duration windowSize,
                                       String windowName) {
        
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);
        
        KTable<Windowed<String>, VolumeAnalytics> volumeAnalytics = tradesStream
            .groupByKey(Grouped.with(Serdes.String(), tradeSerde))
            .windowedBy(timeWindows)
            .aggregate(
                VolumeAnalytics::new,
                this::aggregateVolume,
                Materialized.with(Serdes.String(), new JsonSerde<>(VolumeAnalytics.class))
            );
        
        volumeAnalytics.toStream()
            .map((windowedKey, analytics) -> {
                analytics.setWindowStart(Instant.ofEpochMilli(windowedKey.window().start()));
                analytics.setWindowEnd(Instant.ofEpochMilli(windowedKey.window().end()));
                analytics.calculateMetrics();
                return KeyValue.pair(
                    windowedKey.key() + "-" + windowName,
                    analytics
                );
            })
            .to(volumeAnalyticsTopic,
                Produced.with(Serdes.String(), new JsonSerde<>(VolumeAnalytics.class)));
    }
    
    private VolumeAnalytics aggregateVolume(String key, Trade trade, VolumeAnalytics analytics) {
        analytics.setSymbol(trade.getSymbol());
        analytics.setTotalVolume(analytics.getTotalVolume().add(trade.getVolume()));
        analytics.setTotalTrades(analytics.getTotalTrades() + 1);
        
        if (trade.getIsBuyerMaker()) {
            analytics.setSellVolume(analytics.getSellVolume().add(trade.getVolume()));
            analytics.setSellTrades(analytics.getSellTrades() + 1);
        } else {
            analytics.setBuyVolume(analytics.getBuyVolume().add(trade.getVolume()));
            analytics.setBuyTrades(analytics.getBuyTrades() + 1);
        }
        
        // Track large trades
        if (trade.getVolume().compareTo(analytics.getLargeTradeThreshold()) > 0) {
            analytics.setLargeTrades(analytics.getLargeTrades() + 1);
            analytics.setLargeTradeVolume(analytics.getLargeTradeVolume().add(trade.getVolume()));
        }
        
        return analytics;
    }
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VolumeAnalytics {
        private String symbol;
        private Instant windowStart;
        private Instant windowEnd;
        @Builder.Default
        private BigDecimal totalVolume = BigDecimal.ZERO;
        @Builder.Default
        private BigDecimal buyVolume = BigDecimal.ZERO;
        @Builder.Default
        private BigDecimal sellVolume = BigDecimal.ZERO;
        @Builder.Default
        private Long totalTrades = 0L;
        @Builder.Default
        private Long buyTrades = 0L;
        @Builder.Default
        private Long sellTrades = 0L;
        @Builder.Default
        private Long largeTrades = 0L;
        @Builder.Default
        private BigDecimal largeTradeVolume = BigDecimal.ZERO;
        @Builder.Default
        private BigDecimal largeTradeThreshold = new BigDecimal("10000"); // $10k USD
        
        // Calculated metrics
        private BigDecimal buyVolumeRatio;
        private BigDecimal averageTradeSize;
        private BigDecimal largeTradeVolumeRatio;
        private String volumeTrend; // BULLISH, BEARISH, NEUTRAL
        
        public void calculateMetrics() {
            // Buy volume ratio
            if (totalVolume.compareTo(BigDecimal.ZERO) > 0) {
                buyVolumeRatio = buyVolume.divide(totalVolume, 4, RoundingMode.HALF_UP);
            } else {
                buyVolumeRatio = new BigDecimal("0.5");
            }
            
            // Average trade size
            if (totalTrades > 0) {
                averageTradeSize = totalVolume.divide(
                    new BigDecimal(totalTrades), 8, RoundingMode.HALF_UP
                );
            } else {
                averageTradeSize = BigDecimal.ZERO;
            }
            
            // Large trade volume ratio
            if (totalVolume.compareTo(BigDecimal.ZERO) > 0) {
                largeTradeVolumeRatio = largeTradeVolume.divide(
                    totalVolume, 4, RoundingMode.HALF_UP
                );
            } else {
                largeTradeVolumeRatio = BigDecimal.ZERO;
            }
            
            // Determine volume trend
            if (buyVolumeRatio.compareTo(new BigDecimal("0.6")) > 0) {
                volumeTrend = "BULLISH";
            } else if (buyVolumeRatio.compareTo(new BigDecimal("0.4")) < 0) {
                volumeTrend = "BEARISH";
            } else {
                volumeTrend = "NEUTRAL";
            }
        }
    }
}
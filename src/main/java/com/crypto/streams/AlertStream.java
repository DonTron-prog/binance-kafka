package com.crypto.streams;

import com.crypto.model.AggregatedPrice;
import com.crypto.model.PriceTrend;
import com.crypto.streams.VolumeAnalyticsStream.VolumeAnalytics;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class AlertStream {
    
    @Value("${kafka.topics.aggregated-prices}")
    private String aggregatedPricesTopic;
    
    @Value("${kafka.topics.price-trends}")
    private String priceTrendsTopic;
    
    @Value("${kafka.topics.volume-analytics}")
    private String volumeAnalyticsTopic;
    
    @Value("${kafka.topics.alerts}")
    private String alertsTopic;
    
    private final JsonSerde<AggregatedPrice> aggregatedPriceSerde;
    private final JsonSerde<PriceTrend> priceTrendSerde;
    
    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        // Price alerts
        KStream<String, AggregatedPrice> pricesStream = streamsBuilder.stream(
            aggregatedPricesTopic,
            Consumed.with(Serdes.String(), aggregatedPriceSerde)
        );
        
        pricesStream
            .filter((key, value) -> key.endsWith("-1m"))
            .flatMapValues(this::checkPriceAlerts)
            .to(alertsTopic, Produced.with(Serdes.String(), new JsonSerde<>(Alert.class)));
        
        // Trend alerts
        KStream<String, PriceTrend> trendsStream = streamsBuilder.stream(
            priceTrendsTopic,
            Consumed.with(Serdes.String(), priceTrendSerde)
        );
        
        trendsStream
            .flatMapValues(this::checkTrendAlerts)
            .to(alertsTopic, Produced.with(Serdes.String(), new JsonSerde<>(Alert.class)));
        
        // Volume alerts
        KStream<String, VolumeAnalytics> volumeStream = streamsBuilder.stream(
            volumeAnalyticsTopic,
            Consumed.with(Serdes.String(), new JsonSerde<>(VolumeAnalytics.class))
        );
        
        volumeStream
            .filter((key, value) -> key.endsWith("-1m"))
            .flatMapValues(this::checkVolumeAlerts)
            .to(alertsTopic, Produced.with(Serdes.String(), new JsonSerde<>(Alert.class)));
    }
    
    private List<Alert> checkPriceAlerts(AggregatedPrice price) {
        List<Alert> alerts = new ArrayList<>();
        
        // Check for significant price changes (> 2% in 1 minute)
        BigDecimal priceChangePercent = price.getPriceChangePercent();
        if (priceChangePercent.abs().compareTo(new BigDecimal("2")) > 0) {
            Alert alert = Alert.builder()
                .symbol(price.getSymbol())
                .alertType(AlertType.PRICE_SPIKE)
                .severity(priceChangePercent.abs().compareTo(new BigDecimal("5")) > 0 ? 
                    Severity.HIGH : Severity.MEDIUM)
                .message(String.format("%s price changed by %.2f%% in 1 minute",
                    price.getSymbol(), priceChangePercent))
                .timestamp(Instant.now())
                .build();
            alert.metadata("priceChange", priceChangePercent.toString());
            alert.metadata("currentPrice", price.getClose().toString());
            alerts.add(alert);
        }
        
        return alerts;
    }
    
    private List<Alert> checkTrendAlerts(PriceTrend trend) {
        List<Alert> alerts = new ArrayList<>();
        
        // Check for trend reversals
        if (trend.getSma5() != null && trend.getSma15() != null) {
            BigDecimal crossover = trend.getSma5().subtract(trend.getSma15());
            BigDecimal previousCrossover = trend.getEma5() != null && trend.getEma15() != null ?
                trend.getEma5().subtract(trend.getEma15()) : BigDecimal.ZERO;
            
            // Golden cross (bullish signal)
            if (crossover.compareTo(BigDecimal.ZERO) > 0 && 
                previousCrossover.compareTo(BigDecimal.ZERO) <= 0) {
                Alert alert = Alert.builder()
                    .symbol(trend.getSymbol())
                    .alertType(AlertType.GOLDEN_CROSS)
                    .severity(Severity.HIGH)
                    .message(String.format("%s: Golden cross detected - potential bullish reversal",
                        trend.getSymbol()))
                    .timestamp(Instant.now())
                    .build();
                alert.metadata("sma5", trend.getSma5().toString());
                alert.metadata("sma15", trend.getSma15().toString());
                alerts.add(alert);
            }
            
            // Death cross (bearish signal)
            if (crossover.compareTo(BigDecimal.ZERO) < 0 && 
                previousCrossover.compareTo(BigDecimal.ZERO) >= 0) {
                Alert alert = Alert.builder()
                    .symbol(trend.getSymbol())
                    .alertType(AlertType.DEATH_CROSS)
                    .severity(Severity.HIGH)
                    .message(String.format("%s: Death cross detected - potential bearish reversal",
                        trend.getSymbol()))
                    .timestamp(Instant.now())
                    .build();
                alert.metadata("sma5", trend.getSma5().toString());
                alert.metadata("sma15", trend.getSma15().toString());
                alerts.add(alert);
            }
        }
        
        // Check for overbought/oversold conditions
        if (trend.getRsi() != null) {
            if (trend.getRsi().compareTo(new BigDecimal("70")) > 0) {
                Alert alert = Alert.builder()
                    .symbol(trend.getSymbol())
                    .alertType(AlertType.OVERBOUGHT)
                    .severity(Severity.MEDIUM)
                    .message(String.format("%s: RSI %.2f - Overbought condition",
                        trend.getSymbol(), trend.getRsi()))
                    .timestamp(Instant.now())
                    .build();
                alert.metadata("rsi", trend.getRsi().toString());
                alerts.add(alert);
            } else if (trend.getRsi().compareTo(new BigDecimal("30")) < 0) {
                Alert alert = Alert.builder()
                    .symbol(trend.getSymbol())
                    .alertType(AlertType.OVERSOLD)
                    .severity(Severity.MEDIUM)
                    .message(String.format("%s: RSI %.2f - Oversold condition",
                        trend.getSymbol(), trend.getRsi()))
                    .timestamp(Instant.now())
                    .build();
                alert.metadata("rsi", trend.getRsi().toString());
                alerts.add(alert);
            }
        }
        
        return alerts;
    }
    
    private List<Alert> checkVolumeAlerts(VolumeAnalytics volume) {
        List<Alert> alerts = new ArrayList<>();
        
        // Check for unusual volume
        if (volume.getLargeTradeVolumeRatio() != null && 
            volume.getLargeTradeVolumeRatio().compareTo(new BigDecimal("0.5")) > 0) {
            Alert alert = Alert.builder()
                .symbol(volume.getSymbol())
                .alertType(AlertType.WHALE_ACTIVITY)
                .severity(Severity.HIGH)
                .message(String.format("%s: Large trades account for %.2f%% of volume",
                    volume.getSymbol(), 
                    volume.getLargeTradeVolumeRatio().multiply(new BigDecimal("100"))))
                .timestamp(Instant.now())
                .build();
            alert.metadata("largeTradeVolume", volume.getLargeTradeVolume().toString());
            alert.metadata("totalVolume", volume.getTotalVolume().toString());
            alerts.add(alert);
        }
        
        // Check for volume imbalance
        if (volume.getBuyVolumeRatio() != null) {
            if (volume.getBuyVolumeRatio().compareTo(new BigDecimal("0.7")) > 0) {
                Alert alert = Alert.builder()
                    .symbol(volume.getSymbol())
                    .alertType(AlertType.HEAVY_BUYING)
                    .severity(Severity.MEDIUM)
                    .message(String.format("%s: Heavy buying pressure - %.2f%% buy volume",
                        volume.getSymbol(),
                        volume.getBuyVolumeRatio().multiply(new BigDecimal("100"))))
                    .timestamp(Instant.now())
                    .build();
                alert.metadata("buyVolume", volume.getBuyVolume().toString());
                alert.metadata("sellVolume", volume.getSellVolume().toString());
                alerts.add(alert);
            } else if (volume.getBuyVolumeRatio().compareTo(new BigDecimal("0.3")) < 0) {
                Alert alert = Alert.builder()
                    .symbol(volume.getSymbol())
                    .alertType(AlertType.HEAVY_SELLING)
                    .severity(Severity.MEDIUM)
                    .message(String.format("%s: Heavy selling pressure - %.2f%% sell volume",
                        volume.getSymbol(),
                        BigDecimal.ONE.subtract(volume.getBuyVolumeRatio())
                            .multiply(new BigDecimal("100"))))
                    .timestamp(Instant.now())
                    .build();
                alert.metadata("buyVolume", volume.getBuyVolume().toString());
                alert.metadata("sellVolume", volume.getSellVolume().toString());
                alerts.add(alert);
            }
        }
        
        return alerts;
    }
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Alert {
        private String symbol;
        private AlertType alertType;
        private Severity severity;
        private String message;
        private Instant timestamp;
        @Builder.Default
        private java.util.Map<String, String> metadata = new java.util.HashMap<>();
        
        public Alert metadata(String key, String value) {
            this.metadata.put(key, value);
            return this;
        }
    }
    
    public enum AlertType {
        PRICE_SPIKE,
        GOLDEN_CROSS,
        DEATH_CROSS,
        OVERBOUGHT,
        OVERSOLD,
        WHALE_ACTIVITY,
        HEAVY_BUYING,
        HEAVY_SELLING
    }
    
    public enum Severity {
        LOW, MEDIUM, HIGH, CRITICAL
    }
}
package com.crypto.streams;

import com.crypto.model.AggregatedPrice;
import com.crypto.model.PriceTrend;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class TrendAnalysisStream {
    
    @Value("${kafka.topics.aggregated-prices}")
    private String aggregatedPricesTopic;
    
    @Value("${kafka.topics.price-trends}")
    private String priceTrendsTopic;
    
    private final JsonSerde<AggregatedPrice> aggregatedPriceSerde;
    private final JsonSerde<PriceTrend> priceTrendSerde;
    
    private static final String PRICE_HISTORY_STORE = "price-history-store";
    
    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        // Create state store for price history
        StoreBuilder<KeyValueStore<String, List<BigDecimal>>> storeBuilder = 
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(PRICE_HISTORY_STORE),
                Serdes.String(),
                Serdes.ListSerde(ArrayList.class, new JsonSerde<>(BigDecimal.class))
            );
        
        streamsBuilder.addStateStore(storeBuilder);
        
        KStream<String, AggregatedPrice> pricesStream = streamsBuilder.stream(
            aggregatedPricesTopic,
            Consumed.with(Serdes.String(), aggregatedPriceSerde)
        );
        
        // Process only 1-minute aggregations for trend analysis
        pricesStream
            .filter((key, value) -> key.endsWith("-1m"))
            .transform(() -> new TrendCalculator(), PRICE_HISTORY_STORE)
            .to(priceTrendsTopic, Produced.with(Serdes.String(), priceTrendSerde));
    }
    
    private static class TrendCalculator implements Transformer<String, AggregatedPrice, KeyValue<String, PriceTrend>> {
        private KeyValueStore<String, List<BigDecimal>> priceHistoryStore;
        
        @Override
        public void init(org.apache.kafka.streams.processor.ProcessorContext context) {
            this.priceHistoryStore = context.getStateStore(PRICE_HISTORY_STORE);
        }
        
        @Override
        public KeyValue<String, PriceTrend> transform(String key, AggregatedPrice value) {
            String symbol = value.getSymbol();
            BigDecimal currentPrice = value.getClose();
            
            // Get price history
            List<BigDecimal> priceHistory = priceHistoryStore.get(symbol);
            if (priceHistory == null) {
                priceHistory = new ArrayList<>();
            }
            
            // Add current price to history
            priceHistory.add(currentPrice);
            
            // Keep only last 30 prices
            if (priceHistory.size() > 30) {
                priceHistory.remove(0);
            }
            
            // Update store
            priceHistoryStore.put(symbol, priceHistory);
            
            // Calculate trend indicators
            PriceTrend trend = PriceTrend.builder()
                .symbol(symbol)
                .timestamp(Instant.now())
                .currentPrice(currentPrice)
                .build();
            
            if (priceHistory.size() >= 5) {
                trend.setSma5(calculateSMA(priceHistory, 5));
                trend.setEma5(calculateEMA(priceHistory, 5));
            }
            
            if (priceHistory.size() >= 15) {
                trend.setSma15(calculateSMA(priceHistory, 15));
                trend.setEma15(calculateEMA(priceHistory, 15));
                trend.setRsi(calculateRSI(priceHistory, 14));
            }
            
            if (priceHistory.size() >= 30) {
                trend.setSma30(calculateSMA(priceHistory, 30));
            }
            
            // Calculate momentum
            if (priceHistory.size() >= 10) {
                BigDecimal priceChange = currentPrice.subtract(priceHistory.get(priceHistory.size() - 10));
                trend.setMomentum(priceChange);
            }
            
            // Determine trend direction and strength
            determineTrendDirectionAndStrength(trend);
            
            return KeyValue.pair(symbol, trend);
        }
        
        @Override
        public void close() {
            // Nothing to close
        }
        
        private BigDecimal calculateSMA(List<BigDecimal> prices, int period) {
            int startIndex = Math.max(0, prices.size() - period);
            BigDecimal sum = BigDecimal.ZERO;
            int count = 0;
            
            for (int i = startIndex; i < prices.size(); i++) {
                sum = sum.add(prices.get(i));
                count++;
            }
            
            return sum.divide(new BigDecimal(count), 8, RoundingMode.HALF_UP);
        }
        
        private BigDecimal calculateEMA(List<BigDecimal> prices, int period) {
            BigDecimal multiplier = new BigDecimal(2).divide(
                new BigDecimal(period + 1), 8, RoundingMode.HALF_UP
            );
            
            BigDecimal ema = prices.get(Math.max(0, prices.size() - period));
            
            for (int i = Math.max(1, prices.size() - period + 1); i < prices.size(); i++) {
                BigDecimal currentPrice = prices.get(i);
                ema = currentPrice.subtract(ema)
                    .multiply(multiplier)
                    .add(ema);
            }
            
            return ema;
        }
        
        private BigDecimal calculateRSI(List<BigDecimal> prices, int period) {
            if (prices.size() < period + 1) {
                return new BigDecimal("50");
            }
            
            BigDecimal avgGain = BigDecimal.ZERO;
            BigDecimal avgLoss = BigDecimal.ZERO;
            
            // Calculate initial average gain and loss
            for (int i = prices.size() - period; i < prices.size(); i++) {
                BigDecimal change = prices.get(i).subtract(prices.get(i - 1));
                if (change.compareTo(BigDecimal.ZERO) > 0) {
                    avgGain = avgGain.add(change);
                } else {
                    avgLoss = avgLoss.add(change.abs());
                }
            }
            
            avgGain = avgGain.divide(new BigDecimal(period), 8, RoundingMode.HALF_UP);
            avgLoss = avgLoss.divide(new BigDecimal(period), 8, RoundingMode.HALF_UP);
            
            if (avgLoss.compareTo(BigDecimal.ZERO) == 0) {
                return new BigDecimal("100");
            }
            
            BigDecimal rs = avgGain.divide(avgLoss, 8, RoundingMode.HALF_UP);
            BigDecimal rsi = new BigDecimal("100").subtract(
                new BigDecimal("100").divide(
                    BigDecimal.ONE.add(rs), 8, RoundingMode.HALF_UP
                )
            );
            
            return rsi;
        }
        
        private void determineTrendDirectionAndStrength(PriceTrend trend) {
            if (trend.getSma5() == null || trend.getSma15() == null) {
                trend.setTrendDirection(PriceTrend.TrendDirection.NEUTRAL);
                trend.setTrendStrength(PriceTrend.TrendStrength.WEAK);
                return;
            }
            
            BigDecimal shortTermTrend = trend.getSma5().subtract(trend.getSma15());
            BigDecimal pricePosition = trend.getCurrentPrice().subtract(trend.getSma15());
            
            // Determine direction
            if (shortTermTrend.compareTo(BigDecimal.ZERO) > 0 && 
                pricePosition.compareTo(BigDecimal.ZERO) > 0) {
                trend.setTrendDirection(PriceTrend.TrendDirection.BULLISH);
            } else if (shortTermTrend.compareTo(BigDecimal.ZERO) < 0 && 
                       pricePosition.compareTo(BigDecimal.ZERO) < 0) {
                trend.setTrendDirection(PriceTrend.TrendDirection.BEARISH);
            } else {
                trend.setTrendDirection(PriceTrend.TrendDirection.NEUTRAL);
            }
            
            // Determine strength based on momentum and RSI
            if (trend.getMomentum() != null && trend.getRsi() != null) {
                BigDecimal momentumPercent = trend.getMomentum()
                    .divide(trend.getCurrentPrice(), 4, RoundingMode.HALF_UP)
                    .abs()
                    .multiply(new BigDecimal("100"));
                
                if (momentumPercent.compareTo(new BigDecimal("5")) > 0 ||
                    trend.getRsi().compareTo(new BigDecimal("70")) > 0 ||
                    trend.getRsi().compareTo(new BigDecimal("30")) < 0) {
                    trend.setTrendStrength(PriceTrend.TrendStrength.STRONG);
                } else if (momentumPercent.compareTo(new BigDecimal("2")) > 0) {
                    trend.setTrendStrength(PriceTrend.TrendStrength.MODERATE);
                } else {
                    trend.setTrendStrength(PriceTrend.TrendStrength.WEAK);
                }
            }
        }
    }
}
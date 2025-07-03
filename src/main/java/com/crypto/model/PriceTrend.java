package com.crypto.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PriceTrend {
    
    private String symbol;
    private Instant timestamp;
    private BigDecimal currentPrice;
    private BigDecimal sma5;  // 5-period Simple Moving Average
    private BigDecimal sma15; // 15-period Simple Moving Average
    private BigDecimal sma30; // 30-period Simple Moving Average
    private BigDecimal ema5;  // 5-period Exponential Moving Average
    private BigDecimal ema15; // 15-period Exponential Moving Average
    private BigDecimal rsi;   // Relative Strength Index
    private BigDecimal momentum;
    private TrendDirection trendDirection;
    private TrendStrength trendStrength;
    
    public enum TrendDirection {
        BULLISH, BEARISH, NEUTRAL
    }
    
    public enum TrendStrength {
        STRONG, MODERATE, WEAK
    }
}
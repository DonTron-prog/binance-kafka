package com.crypto.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Trade {
    
    @JsonProperty("e")
    private String eventType;
    
    @JsonProperty("E")
    private Long eventTime;
    
    @JsonProperty("s")
    private String symbol;
    
    @JsonProperty("t")
    private Long tradeId;
    
    @JsonProperty("p")
    private BigDecimal price;
    
    @JsonProperty("q")
    private BigDecimal quantity;
    
    @JsonProperty("b")
    private Long buyerOrderId;
    
    @JsonProperty("a")
    private Long sellerOrderId;
    
    @JsonProperty("T")
    private Long tradeTime;
    
    @JsonProperty("m")
    private Boolean isBuyerMaker;
    
    public BigDecimal getVolume() {
        return price.multiply(quantity);
    }
    
    public Instant getTradeInstant() {
        return Instant.ofEpochMilli(tradeTime);
    }
}
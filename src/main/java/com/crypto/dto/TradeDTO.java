package com.crypto.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeDTO {
    private String symbol;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal volume;
    private Long tradeTime;
    private Boolean isBuyerMaker;
    private String eventType;
    private Long eventTime;
    private Long tradeId;
}
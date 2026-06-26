package com.example.ordermanagement.model;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Builder(toBuilder = true)
public record OrderPlaced(
    String orderId,
    String customerId,
    String productName,
    Integer quantity,
    BigDecimal price,
    LocalDateTime timestamp
) {
}


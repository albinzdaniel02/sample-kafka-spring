package com.example.ordermanagement.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class OrderPlacedTest {

    @Test
    void testSerializationAndDeserialization() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        OrderPlaced order = OrderPlaced.builder()
                .orderId("order-123")
                .customerId("cust-456")
                .productName("Laptop")
                .quantity(2)
                .price(new BigDecimal("1200.50"))
                .timestamp(LocalDateTime.of(2026, 6, 26, 9, 30, 0))
                .build();

        String json = objectMapper.writeValueAsString(order);
        assertNotNull(json);

        OrderPlaced deserializedOrder = objectMapper.readValue(json, OrderPlaced.class);
        assertEquals(order.getOrderId(), deserializedOrder.getOrderId());
        assertEquals(order.getCustomerId(), deserializedOrder.getCustomerId());
        assertEquals(order.getProductName(), deserializedOrder.getProductName());
        assertEquals(order.getQuantity(), deserializedOrder.getQuantity());
        assertEquals(order.getPrice(), deserializedOrder.getPrice());
        assertEquals(order.getTimestamp(), deserializedOrder.getTimestamp());
    }
}

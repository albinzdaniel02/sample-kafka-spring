package com.example.ordermanagement.controller;

import com.example.ordermanagement.model.OrderPlaced;
import com.example.ordermanagement.producer.OrderProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Validated
public class OrderController {

    private final OrderProducerService orderProducerService;

    @PostMapping
    public ResponseEntity<OrderPlaced> createOrder(@Valid @RequestBody OrderPlaced orderPlaced) {
        log.info("Received request to place order: {}", orderPlaced);
        
        String orderId = orderPlaced.orderId();
        if (orderId == null || orderId.isBlank()) {
            orderId = UUID.randomUUID().toString();
        }
        
        LocalDateTime timestamp = orderPlaced.timestamp();
        if (timestamp == null) {
            timestamp = LocalDateTime.now();
        }

        OrderPlaced updatedOrder = orderPlaced.toBuilder()
                .orderId(orderId)
                .timestamp(timestamp)
                .build();

        orderProducerService.sendOrderPlaced(updatedOrder);
        
        return ResponseEntity.ok(updatedOrder);
    }
}

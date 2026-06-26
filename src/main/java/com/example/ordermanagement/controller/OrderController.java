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
        
        if (orderPlaced.getOrderId() == null || orderPlaced.getOrderId().isBlank()) {
            orderPlaced.setOrderId(UUID.randomUUID().toString());
        }
        if (orderPlaced.getTimestamp() == null) {
            orderPlaced.setTimestamp(LocalDateTime.now());
        }

        orderProducerService.sendOrderPlaced(orderPlaced);
        
        return ResponseEntity.ok(orderPlaced);
    }
}

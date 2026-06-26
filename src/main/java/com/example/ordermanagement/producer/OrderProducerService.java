package com.example.ordermanagement.producer;

import com.example.ordermanagement.model.OrderPlaced;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {

    private final KafkaTemplate<String, OrderPlaced> kafkaTemplate;
    private static final String TOPIC = "orders";

    public void sendOrderPlaced(OrderPlaced orderPlaced) {
        String key = orderPlaced.getCustomerId();
        log.info("Sending OrderPlaced event: key={}, order={}", key, orderPlaced);
        kafkaTemplate.send(TOPIC, key, orderPlaced)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Successfully sent OrderPlaced event to topic orders, partition={}, offset={}",
                                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to send OrderPlaced event to topic orders", ex);
                    }
                });
    }
}

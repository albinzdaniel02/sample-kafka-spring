package com.example.ordermanagement.consumer;

import com.example.ordermanagement.model.OrderPlaced;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class OrderDltConsumerService {

    private final List<DltRecord> dltRecords = Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(topics = "orders.DLT", groupId = "order-dlt-group")
    public void consume(@Payload OrderPlaced order,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset,
                        @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("Consumed DLT OrderPlaced event: {} on partition: {}, offset: {}, key: {}", order, partition, offset, key);
        dltRecords.add(new DltRecord(order, partition, offset, key));
    }

    public List<DltRecord> getDltRecords() {
        return new ArrayList<>(dltRecords);
    }

    public void clearDltRecords() {
        dltRecords.clear();
    }

    public record DltRecord(OrderPlaced order, int partition, long offset, String key) {}
}

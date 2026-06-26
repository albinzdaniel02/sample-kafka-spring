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
public class OrderConsumerService {

    private final List<ConsumedRecord> consumedRecords = Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(topics = "orders", groupId = "order-group")
    public void consume(@Payload OrderPlaced order,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset,
                        @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("Consumed OrderPlaced event: {} on partition: {}, offset: {}, key: {}", order, partition, offset, key);
        consumedRecords.add(new ConsumedRecord(order, partition, offset, key));
    }

    public List<ConsumedRecord> getConsumedRecords() {
        return new ArrayList<>(consumedRecords);
    }

    public void clearConsumedRecords() {
        consumedRecords.clear();
    }

    public static class ConsumedRecord {
        private final OrderPlaced order;
        private final int partition;
        private final long offset;
        private final String key;

        public ConsumedRecord(OrderPlaced order, int partition, long offset, String key) {
            this.order = order;
            this.partition = partition;
            this.offset = offset;
            this.key = key;
        }

        public OrderPlaced getOrder() {
            return order;
        }

        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }

        public String getKey() {
            return key;
        }
    }
}

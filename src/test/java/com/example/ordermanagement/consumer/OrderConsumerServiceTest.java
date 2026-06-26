package com.example.ordermanagement.consumer;

import com.example.ordermanagement.model.OrderPlaced;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = { "orders", "orders.DLT" }, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@ActiveProfiles("test")
@DirtiesContext
class OrderConsumerServiceTest {

    @Autowired
    private KafkaTemplate<String, OrderPlaced> kafkaTemplate;

    @Autowired
    private OrderConsumerService orderConsumerService;

    @Autowired
    private OrderDltConsumerService orderDltConsumerService;

    @BeforeEach
    void setUp() {
        orderConsumerService.clearConsumedRecords();
        orderDltConsumerService.clearDltRecords();
    }

    @Test
    void testPartitionOrderingGuarantees() throws Exception {
        int messageCount = 10;
        List<CompletableFuture<?>> futures = new ArrayList<>();

        // Send messages for cust-A and cust-B concurrently
        for (int i = 1; i <= messageCount; i++) {
            final int seq = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    OrderPlaced order = OrderPlaced.builder()
                            .orderId("A-" + seq)
                            .customerId("cust-A")
                            .productName("Product A-" + seq)
                            .quantity(seq)
                            .price(new BigDecimal("10.00").multiply(new BigDecimal(seq)))
                            .timestamp(LocalDateTime.now())
                            .build();
                    kafkaTemplate.send("orders", "cust-A", order).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));

            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    OrderPlaced order = OrderPlaced.builder()
                            .orderId("B-" + seq)
                            .customerId("cust-B")
                            .productName("Product B-" + seq)
                            .quantity(seq)
                            .price(new BigDecimal("20.00").multiply(new BigDecimal(seq)))
                            .timestamp(LocalDateTime.now())
                            .build();
                    kafkaTemplate.send("orders", "cust-B", order).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Wait for all sends to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

        // Wait for all messages to be consumed
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(orderConsumerService.getConsumedRecords()).hasSize(messageCount * 2);
        });

        List<OrderConsumerService.ConsumedRecord> records = orderConsumerService.getConsumedRecords();

        // Verify cust-A records
        List<OrderConsumerService.ConsumedRecord> custARecords = records.stream()
                .filter(r -> "cust-A".equals(r.getKey()))
                .collect(Collectors.toList());

        assertThat(custARecords).hasSize(messageCount);
        int expectedPartitionA = custARecords.get(0).getPartition();
        
        // Assert that all cust-A records went to the same partition
        for (OrderConsumerService.ConsumedRecord r : custARecords) {
            assertThat(r.getPartition()).isEqualTo(expectedPartitionA);
        }

        // Assert that cust-A records are processed in the strict order they were sent (increasing offsets)
        List<OrderConsumerService.ConsumedRecord> sortedByOffsetA = new ArrayList<>(custARecords);
        sortedByOffsetA.sort(Comparator.comparingLong(OrderConsumerService.ConsumedRecord::getOffset));

        for (int i = 0; i < messageCount; i++) {
            assertThat(custARecords.get(i).getOffset()).isEqualTo(sortedByOffsetA.get(i).getOffset());
        }

        // Verify cust-B records
        List<OrderConsumerService.ConsumedRecord> custBRecords = records.stream()
                .filter(r -> "cust-B".equals(r.getKey()))
                .collect(Collectors.toList());

        assertThat(custBRecords).hasSize(messageCount);
        int expectedPartitionB = custBRecords.get(0).getPartition();

        // Assert that all cust-B records went to the same partition
        for (OrderConsumerService.ConsumedRecord r : custBRecords) {
            assertThat(r.getPartition()).isEqualTo(expectedPartitionB);
        }

        List<OrderConsumerService.ConsumedRecord> sortedByOffsetB = new ArrayList<>(custBRecords);
        sortedByOffsetB.sort(Comparator.comparingLong(OrderConsumerService.ConsumedRecord::getOffset));

        for (int i = 0; i < messageCount; i++) {
            assertThat(custBRecords.get(i).getOffset()).isEqualTo(sortedByOffsetB.get(i).getOffset());
        }
        
        // Verify strict offset increment in consumed sequence to guarantee ordering is maintained
        for (int i = 1; i < custARecords.size(); i++) {
            assertThat(custARecords.get(i).getOffset()).isGreaterThan(custARecords.get(i-1).getOffset());
        }
        for (int i = 1; i < custBRecords.size(); i++) {
            assertThat(custBRecords.get(i).getOffset()).isGreaterThan(custBRecords.get(i-1).getOffset());
        }
    }

    @Test
    void testPoisonMessageAndDltRecovery() throws Exception {
        OrderPlaced poisonOrder = OrderPlaced.builder()
                .orderId("poison-1")
                .customerId("cust-poison")
                .productName("Poison Product")
                .quantity(-1) // Invalid quantity
                .price(new BigDecimal("99.99"))
                .timestamp(LocalDateTime.now())
                .build();

        kafkaTemplate.send("orders", "cust-poison", poisonOrder).get();

        // Wait for it to be routed to DLT and consumed by OrderDltConsumerService
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(orderDltConsumerService.getDltRecords()).hasSize(1);
        });

        OrderDltConsumerService.DltRecord dltRecord = orderDltConsumerService.getDltRecords().get(0);
        assertThat(dltRecord.getOrder().getOrderId()).isEqualTo("poison-1");
        assertThat(dltRecord.getKey()).isEqualTo("cust-poison");
    }
}

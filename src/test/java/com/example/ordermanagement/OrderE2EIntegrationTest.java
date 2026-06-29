package com.example.ordermanagement;

import com.example.ordermanagement.consumer.OrderConsumerService;
import com.example.ordermanagement.consumer.OrderDltConsumerService;
import com.example.ordermanagement.model.OrderPlaced;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class OrderE2EIntegrationTest {

    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1")
    );

    static {
        kafka.start();
    }

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private OrderConsumerService orderConsumerService;

    @Autowired
    private OrderDltConsumerService orderDltConsumerService;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        orderConsumerService.clearConsumedRecords();
        orderDltConsumerService.clearDltRecords();
    }

    @Test
    void testEndToEndOrderFlowWithKafkaAndDlt() throws Exception {
        // 1. Post a valid order to the REST controller
        OrderPlaced validOrder = OrderPlaced.builder()
                .customerId("cust-789")
                .productName("Ultra Widget")
                .quantity(10)
                .price(new BigDecimal("49.99"))
                .timestamp(LocalDateTime.now())
                .build();

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validOrder)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").isNotEmpty())
                .andExpect(jsonPath("$.customerId").value("cust-789"))
                .andExpect(jsonPath("$.productName").value("Ultra Widget"))
                .andExpect(jsonPath("$.quantity").value(10))
                .andExpect(jsonPath("$.price").value(49.99));

        // 2. Verify consumption, serialization/deserialization by the main consumer
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(orderConsumerService.getConsumedRecords()).isNotEmpty();
        });

        OrderConsumerService.ConsumedRecord record = orderConsumerService.getConsumedRecords().stream()
                .filter(r -> "cust-789".equals(r.key()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected record not found in consumed records"));

        assertThat(record.order().productName()).isEqualTo("Ultra Widget");
        assertThat(record.order().quantity()).isEqualTo(10);
        assertThat(record.order().price()).isEqualByComparingTo(new BigDecimal("49.99"));
        assertThat(record.order().orderId()).isNotEmpty();

        // 3. Post an invalid order (negative quantity) to test DLT fallback
        OrderPlaced invalidOrder = OrderPlaced.builder()
                .customerId("cust-invalid")
                .productName("Broken Item")
                .quantity(-3) // Invalid quantity, triggers InvalidOrderException
                .price(new BigDecimal("5.00"))
                .timestamp(LocalDateTime.now())
                .build();

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalidOrder)))
                .andExpect(status().isOk());

        // 4. Verify DLT fallback behavior - consumed by DLT consumer
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(orderDltConsumerService.getDltRecords()).isNotEmpty();
        });

        OrderDltConsumerService.DltRecord dltRecord = orderDltConsumerService.getDltRecords().stream()
                .filter(r -> "cust-invalid".equals(r.key()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected invalid record not found in DLT records"));

        assertThat(dltRecord.order().productName()).isEqualTo("Broken Item");
        assertThat(dltRecord.order().quantity()).isEqualTo(-3);
        assertThat(dltRecord.order().price()).isEqualByComparingTo(new BigDecimal("5.00"));
        assertThat(dltRecord.order().orderId()).isNotEmpty();
    }
}

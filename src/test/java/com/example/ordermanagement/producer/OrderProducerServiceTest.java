package com.example.ordermanagement.producer;

import com.example.ordermanagement.model.OrderPlaced;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = { "orders" }, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@ActiveProfiles("test")
class OrderProducerServiceTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private Consumer<String, OrderPlaced> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        
        JsonDeserializer<OrderPlaced> valueDeserializer = new JsonDeserializer<>(OrderPlaced.class, objectMapper);
        valueDeserializer.addTrustedPackages("com.example.ordermanagement.model");

        DefaultKafkaConsumerFactory<String, OrderPlaced> consumerFactory = 
                new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), valueDeserializer);
        
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton("orders"));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void testCreateOrderAndPublishToKafka() throws Exception {
        OrderPlaced orderPlaced = OrderPlaced.builder()
                .customerId("cust-123")
                .productName("Sample Product")
                .quantity(2)
                .price(new BigDecimal("99.99"))
                .build();

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(orderPlaced)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").isNotEmpty())
                .andExpect(jsonPath("$.customerId").value("cust-123"))
                .andExpect(jsonPath("$.productName").value("Sample Product"))
                .andExpect(jsonPath("$.quantity").value(2))
                .andExpect(jsonPath("$.price").value(99.99))
                .andExpect(jsonPath("$.timestamp").isNotEmpty());

        // Verify the message on Kafka
        ConsumerRecord<String, OrderPlaced> record = KafkaTestUtils.getSingleRecord(consumer, "orders", Duration.ofSeconds(10));
        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo("cust-123");
        
        OrderPlaced publishedOrder = record.value();
        assertThat(publishedOrder).isNotNull();
        assertThat(publishedOrder.getCustomerId()).isEqualTo("cust-123");
        assertThat(publishedOrder.getProductName()).isEqualTo("Sample Product");
        assertThat(publishedOrder.getQuantity()).isEqualTo(2);
        assertThat(publishedOrder.getPrice()).isEqualByComparingTo(new BigDecimal("99.99"));
        assertThat(publishedOrder.getOrderId()).isNotEmpty();
        assertThat(publishedOrder.getTimestamp()).isNotNull();
    }
}

package com.example.ordermanagement.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class KafkaConfigTest {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private NewTopic ordersTopic;

    @Autowired
    private NewTopic ordersDltTopic;

    @Autowired
    private org.springframework.kafka.listener.DeadLetterPublishingRecoverer deadLetterPublishingRecoverer;

    @Autowired
    private org.springframework.kafka.listener.DefaultErrorHandler errorHandler;

    @Test
    void testKafkaConfigBeans() {
        assertNotNull(deadLetterPublishingRecoverer);
        assertNotNull(errorHandler);
        assertNotNull(kafkaAdmin);

        assertNotNull(ordersTopic);
        assertEquals("orders", ordersTopic.name());
        assertEquals(3, ordersTopic.numPartitions());
        assertEquals(1, ordersTopic.replicationFactor());

        assertNotNull(ordersDltTopic);
        assertEquals("orders.DLT", ordersDltTopic.name());
        assertEquals(3, ordersDltTopic.numPartitions());
        assertEquals(1, ordersDltTopic.replicationFactor());
    }
}

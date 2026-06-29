package com.example.ordermanagement.config;

import com.example.ordermanagement.model.OrderPlaced;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.properties.spring.json.trusted.packages:com.example.ordermanagement.model}")
    private String trustedPackages;

    // Producer Settings
    @Value("${spring.kafka.producer.key-serializer:org.apache.kafka.common.serialization.StringSerializer}")
    private String keySerializer;

    @Value("${spring.kafka.producer.value-serializer:org.springframework.kafka.support.serializer.JsonSerializer}")
    private String valueSerializer;

    @Value("${spring.kafka.producer.properties.max.block.ms:5000}")
    private String maxBlockMs;

    @Value("${spring.kafka.producer.properties.request.timeout.ms:3000}")
    private String producerRequestTimeout;

    @Value("${spring.kafka.producer.properties.delivery.timeout.ms:120000}")
    private String deliveryTimeout;

    // Consumer Settings
    @Value("${spring.kafka.consumer.key-deserializer:org.apache.kafka.common.serialization.StringDeserializer}")
    private String keyDeserializer;

    @Value("${spring.kafka.consumer.value-deserializer:org.springframework.kafka.support.serializer.JsonDeserializer}")
    private String valueDeserializer;

    @Value("${spring.kafka.consumer.properties.request.timeout.ms:30000}")
    private String consumerRequestTimeout;

    @Value("${spring.kafka.consumer.properties.session.timeout.ms:45000}")
    private String sessionTimeout;

    @Value("${spring.kafka.consumer.properties.fetch.min.bytes:1}")
    private String fetchMinBytes;

    @Value("${spring.kafka.consumer.properties.fetch.max.wait.ms:500}")
    private String fetchMaxWait;

    @Value("${spring.kafka.consumer.properties.max.partition.fetch.bytes:1048576}")
    private String maxPartitionFetchBytes;

    // Concurrency Settings
    @Value("${spring.kafka.listener.concurrency:3}")
    private Integer concurrency;

    @Bean
    public ObjectMapper jackson2ObjectMapper() {
        log.info("Configuring Shared com.fasterxml.jackson.databind.ObjectMapper");
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        log.info("Configuring KafkaAdmin with bootstrap servers: {}", bootstrapServers);
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic ordersTopic() {
        log.info("Configuring Topic: orders");
        return TopicBuilder.name("orders")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ordersDltTopic() {
        log.info("Configuring Topic: orders.DLT");
        return TopicBuilder.name("orders.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public ProducerFactory<String, OrderPlaced> producerFactory(ObjectMapper objectMapper) {
        log.info("Configuring ProducerFactory with timeouts");
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        
        // Optimizations
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerRequestTimeout);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout);

        DefaultKafkaProducerFactory<String, OrderPlaced> factory = new DefaultKafkaProducerFactory<>(configProps);
        factory.setValueSerializer(new JsonSerializer<>(objectMapper));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, OrderPlaced> kafkaTemplate(ProducerFactory<String, OrderPlaced> producerFactory) {
        log.info("Configuring KafkaTemplate");
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, OrderPlaced> consumerFactory(ObjectMapper objectMapper) {
        log.info("Configuring ConsumerFactory with timeouts and fetch sizes");
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, trustedPackages);

        // Optimizations
        configProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, consumerRequestTimeout);
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait);
        configProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);

        return new DefaultKafkaConsumerFactory<>(
                configProps,
                new StringDeserializer(),
                new JsonDeserializer<>(OrderPlaced.class, objectMapper)
        );
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, OrderPlaced> template) {
        log.info("Configuring DeadLetterPublishingRecoverer");
        return new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition("orders.DLT", record.partition()));
    }

    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        log.info("Configuring DefaultErrorHandler");
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderPlaced> kafkaListenerContainerFactory(
            ConsumerFactory<String, OrderPlaced> consumerFactory,
            DefaultErrorHandler errorHandler) {
        log.info("Configuring ConcurrentKafkaListenerContainerFactory");
        ConcurrentKafkaListenerContainerFactory<String, OrderPlaced> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);

        if (concurrency != null) {
            log.info("Setting consumer concurrency to configured value: {}", concurrency);
            factory.setConcurrency(concurrency);
        } else {
            log.info("Setting default consumer concurrency to: 3");
            factory.setConcurrency(3);
        }

        return factory;
    }
}

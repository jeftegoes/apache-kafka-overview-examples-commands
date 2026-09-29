package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducer {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    KafkaTemplate<String, Customer> kafkaTemplate;

    private final String KAFKA_TOPIC = "customer-topic";

    public KafkaProducer(KafkaTemplate<String, Customer> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String createCustomer(Customer customer) {
        String customerId = UUID.randomUUID().toString();

        CompletableFuture<SendResult<String, Customer>> future = this.kafkaTemplate.send(KAFKA_TOPIC, customerId, customer);

        future.whenComplete((result, exception) -> {
            if (exception != null) {
                this.logger.error("Failed to send message: {}", exception.getMessage());
                return;
            }

            this.logger.info("Message sent successfully: {}", result.getRecordMetadata());
        });

        this.logger.info("customerId: {}", customerId);

        return customerId;
    }
}
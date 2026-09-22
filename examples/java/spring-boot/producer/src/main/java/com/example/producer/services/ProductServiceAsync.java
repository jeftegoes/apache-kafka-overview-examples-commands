package com.example.producer.services;

import com.example.producer.events.ProductEvent;
import com.example.producer.requests.ProductRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class ProductServiceAsync {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    KafkaTemplate<String, ProductEvent> kafkaTemplate;

    private final String KAFKA_TOPIC = "product-created-events-topic";

    public ProductServiceAsync(KafkaTemplate<String, ProductEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String createProduct(ProductRequest productRequest) {
        String productId = UUID.randomUUID().toString();

        ProductEvent productEvent = new ProductEvent(productId,
                productRequest.getTitle(),
                productRequest.getPrice(),
                productRequest.getQuantity());

        CompletableFuture<SendResult<String, ProductEvent>> future = this.kafkaTemplate.send(KAFKA_TOPIC, productId, productEvent);

        future.whenComplete((result, exception) -> {
            if (exception != null) {
                this.logger.error("Failed to send message: {}", exception.getMessage());
                return;
            }

            this.logger.info("Message sent successfully: {}", result.getRecordMetadata());
        });

        this.logger.info("productId: {}", productId);

        return productId;
    }
}

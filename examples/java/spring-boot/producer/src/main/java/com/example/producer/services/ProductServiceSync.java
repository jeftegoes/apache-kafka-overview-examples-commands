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
import java.util.concurrent.ExecutionException;

@Service
public class ProductServiceSync {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    KafkaTemplate<String, ProductEvent> kafkaTemplate;

    private final String KAFKA_TOPIC = "product-created-events-topic";

    public ProductServiceSync(KafkaTemplate<String, ProductEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String createProduct(ProductRequest productRequest) throws ExecutionException, InterruptedException {
        String productId = UUID.randomUUID().toString();

        ProductEvent productEvent = new ProductEvent(productId,
                productRequest.getTitle(),
                productRequest.getPrice(),
                productRequest.getQuantity());

        this.logger.info("Before publishing...");

        SendResult<String, ProductEvent> result = this.kafkaTemplate.send(KAFKA_TOPIC, productId, productEvent).get();

        logger.info("Topic: {}, Partition: {}, Offset: {}",
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());

        this.logger.info("productId: {}", productId);

        return productId;
    }
}

package com.example.producer.services;

import com.example.producer.events.ProductEvent;
import com.example.producer.requests.ProductRequest;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
public class ProductService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    KafkaTemplate<String, ProductEvent> kafkaTemplate;

    private final String KAFKA_TOPIC = "product-created-events-topic";

    public ProductService(KafkaTemplate<String, ProductEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String createProduct(ProductRequest productRequest) throws ExecutionException, InterruptedException {
        String productId = UUID.randomUUID().toString();

        ProductEvent productCreatedEvent = new ProductEvent(productId,
                productRequest.getTitle(),
                productRequest.getPrice(),
                productRequest.getQuantity());

        logger.info("Before publishing a ProductCreatedEvent");

        ProducerRecord<String, ProductEvent> record = new ProducerRecord<>(
                KAFKA_TOPIC,
                productId,
                productCreatedEvent);

        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        SendResult<String, ProductEvent> result = kafkaTemplate.send(record).get();

        logger.info("Partition: " + result.getRecordMetadata().partition());
        logger.info("Topic: " + result.getRecordMetadata().topic());
        logger.info("Offset: " + result.getRecordMetadata().offset());

        logger.info("***** Returning product id");

        return productId;
    }
}

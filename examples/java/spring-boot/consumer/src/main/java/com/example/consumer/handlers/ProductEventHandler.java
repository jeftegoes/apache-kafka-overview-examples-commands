package com.example.consumer.handlers;

import com.example.consumer.events.ProductEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ProductEventHandler {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = "product-created-events-topic")
    public void handle(ProductEvent productRequest) {
        logger.info("Received product event: {}", productRequest);
    }
}

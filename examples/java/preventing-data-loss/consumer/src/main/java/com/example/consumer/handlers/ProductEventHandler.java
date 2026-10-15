package com.example.consumer.handlers;

import com.example.consumer.events.ProductEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class ProductEventHandler {
    private RestTemplate restTemplate = new RestTemplate();

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    @KafkaListener(topics = "product-created-events-topic")
    public void handle(@Payload ProductEvent productEvent,
                       Acknowledgment acknowledgment) {

        logger.info("Try to process message");
        try {
            logger.info("Processed value: " + productEvent.getProductId());
            acknowledgment.acknowledge();
        } catch (Exception e) {
            logger.error("Error while processing message: {}" + productEvent.getProductId());
            acknowledgment.acknowledge();
        }
    }
}

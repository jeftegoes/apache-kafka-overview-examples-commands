package com.example.listeners;

import com.example.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CustomerListener {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = "customer-topic")
    public void handle(Customer customer) {
        logger.info("Received product event: {}", customer);
    }
}

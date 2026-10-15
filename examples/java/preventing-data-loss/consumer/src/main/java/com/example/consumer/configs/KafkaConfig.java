package com.example.consumer.configs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Bean
    public DefaultErrorHandler errorHandler() {
        BackOff fixedBackOff = new FixedBackOff(5000, 3);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler((consumerRecord, exception) -> {
            logger.error("Couldn't process message: {}; {}", consumerRecord.value().toString(), exception.toString());
        }, fixedBackOff);

        errorHandler.addNotRetryableExceptions(NullPointerException.class);
        return errorHandler;
    }
}

package com.example.consumer.handlers;

import com.example.consumer.events.ProductEvent;
import com.example.consumer.exceptions.NotRetryableException;
import com.example.consumer.exceptions.RetryableException;
import com.example.consumer.models.ProcessedEventEntity;
import com.example.consumer.repositories.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
public class ProductEventHandler {
    @Autowired
    ProcessedEventRepository processedEventRepository;

    private RestTemplate restTemplate = new RestTemplate();

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    @KafkaListener(topics = "product-created-events-topic")
    public void handle(@Payload ProductEvent productEvent,
                       @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String receivedKey) {

        logger.info("Received a new event: " + productEvent.getTitle() + " with productId: "
                + productEvent.getProductId());

        ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);

        if (existingRecord != null) {
            logger.info("Found a duplicate message id: {}", existingRecord.getMessageId());
            return;
        }

        String requestUrl = "https://viacep.com.br/ws/01001000/json/";

        try {
            ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);

            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                logger.info("Received response from a remote service: " + response.getBody());
            }
        } catch (ResourceAccessException ex) {
            logger.error(ex.getMessage());
            throw new RetryableException(ex);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            throw new NotRetryableException(ex);
        }

        try {
            processedEventRepository.save(new ProcessedEventEntity(messageId, productEvent.getProductId()));
        } catch (DataIntegrityViolationException ex) {
            throw new NotRetryableException(ex);
        }
    }
}

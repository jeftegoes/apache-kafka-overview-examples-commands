package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.models.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class MainCustomObject {
    private static final Logger log = LoggerFactory.getLogger(MainCustomObject.class);

    private static String TOPIC = "demo-topic-custom-object";

    static void main() throws JsonProcessingException {
        KafkaConsumer<String, String> consumer = KafkaManager.getStringStringKafkaConsumer();

        consumer.subscribe(Arrays.asList(TOPIC));

        ObjectMapper objectMapper = new ObjectMapper();

        while (true) {
            log.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                Book book = objectMapper.readValue(record.value(), Book.class);
                log.info("Key: {} | Value: {}", record.key(), record.value());
                log.info("Partition: {} | Offset: {}", record.partition(), record.offset());
                log.info("Book.id: {} | Book.name: {}", book.getId(), book.getName());
            }
        }
    }
}
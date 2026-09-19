package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.models.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainCustomObject {
    private static final Logger log = LoggerFactory.getLogger(MainCustomObject.class);

    static void main() throws InterruptedException, JsonProcessingException {
        KafkaProducer<String, String> producer = KafkaManager.getStringStringKafkaProducer();
        ObjectMapper objectMapper = new ObjectMapper();

        for (int j = 0; j < 5; j++) {
            for (int i = 0; i < 20; i++) {
                String topic = "demo-topic-custom-object";
                Book book = new Book(i, "Book " + i);
                String jsonValue = objectMapper.writeValueAsString(book);

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,String.valueOf(i),  jsonValue);

                producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
                    if (exception == null) {
                        log.info("Sent successfully! Topic: {} | Partition: {} | Offset: {}",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        System.err.println("Error while producing message: " + exception.getMessage());
                    }
                });
            }

            Thread.sleep(500);
        }

        producer.flush();

        producer.close();
    }
}
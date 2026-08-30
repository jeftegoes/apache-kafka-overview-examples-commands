package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainKeys {
    private static final Logger log = LoggerFactory.getLogger(MainKeys.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "my-test-topic";

    static void main() throws InterruptedException {
        KafkaProducer<String, String> producer = KafkaProducerManager.getStringStringKafkaProducer();

        TopicManager.createTopicIfNotExists(BOOTSTRAP_SERVERS, TOPIC, 5, (short) 1);

        for (int j = 0; j < 5; j++) {
            for (int i = 0; i < 90; i++) {
                String key = "key-" + i + "-" + j;
                String value = "Hello Kafka 4.0 - Message #" + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, key, value);

                producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
                    if (exception == null) {
                        log.info("Sent successfully! Key: {} | Topic: {} | Partition: {} | Offset: {}",
                                key, metadata.topic(), metadata.partition(), metadata.offset());
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
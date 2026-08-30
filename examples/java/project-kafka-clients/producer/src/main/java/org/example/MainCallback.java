package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainCallback {
    private static final Logger log = LoggerFactory.getLogger(MainCallback.class);

    static void main() throws InterruptedException {

        KafkaProducer<String, String> producer = KafkaProducerManager.getStringStringKafkaProducer();

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                String topic = "demo-topic";
                String value = "Hello Kafka 4.0 - Message #" + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, value);

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
package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "my-test-topic";

    static void main() {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, "key", "hello world");

        producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
            if (exception == null) {
                System.out.printf("Sent successfully! Topic: %s | Partition: %d | Offset: %d%n",
                        metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                System.err.println("Error while producing message: " + exception.getMessage());
            }
        });

        producer.flush();

        producer.close();
    }
}

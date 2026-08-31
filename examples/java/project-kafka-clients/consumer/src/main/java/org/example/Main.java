package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static String TOPIC = "my-test-topic";

    static void main() {
        KafkaConsumer<String, String> consumer = KafkaManager.getStringStringKafkaConsumer();

        consumer.subscribe(Arrays.asList(TOPIC));

        while (true) {
            log.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: {} | Value: {}", record.key(), record.value());
                log.info("Partition: {} | Offset: {}", record.partition(), record.offset());
            }
        }
    }
}
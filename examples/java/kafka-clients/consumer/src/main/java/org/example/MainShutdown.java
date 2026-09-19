package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class MainShutdown {
    private static final Logger log = LoggerFactory.getLogger(MainShutdown.class);

    private static String TOPIC = "my-test-topic";

    static void main() {
        KafkaConsumer<String, String> consumer = KafkaManager.getStringStringKafkaConsumer();

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal detected. Closing consumer...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            consumer.subscribe(Arrays.asList(TOPIC));

            while (true) {
                log.info("Polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: {} | Value: {}", record.key(), record.value());
                    log.info("Partition: {} | Offset: {}", record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down.");
        } catch (Exception e) {
            log.error("Unexpected exception in the Consumer: {}", e.getMessage());
        } finally {
            consumer.close();
            log.info("The Consumer is now gracefully shut down.");
        }
    }
}
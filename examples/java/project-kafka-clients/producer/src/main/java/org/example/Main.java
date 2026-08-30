package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main {
    static void main() throws InterruptedException {
        KafkaProducer<String, String> producer = KafkaProducerManager.getStringStringKafkaProducer();

        String topic = "demo-topic";
        String value = "hello world";

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, value);

        producer.send(producerRecord);

        Thread.sleep(500);

        producer.flush();

        producer.close();
    }
}

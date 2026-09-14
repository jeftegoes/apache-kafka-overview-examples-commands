import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String GROUP_ID = "my-consumer-avro-application";
    private static final String EARLIEST = "earliest";
    private static final String SPECIFIC_AVRO_READER_KEY = "specific.avro.reader";
    private static final String TOPIC = "customer-avro-topic";

    static void main() {
        System.setProperty(
                "org.apache.avro.SERIALIZABLE_CLASSES",
                "Customer"
        );

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        properties.setProperty(SCHEMA_REGISTRY_URL_KEY, SCHEMA_REGISTRY_URL);
        properties.put(SPECIFIC_AVRO_READER_KEY, true);

        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(TOPIC));

        while (true) {
            log.info("Polling");

            ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, Customer> record : records) {
                Customer customer = record.value();
                log.info("Customer: {}", customer);
                log.info("Key: {} | Value: {}", record.key(), record.value());
                log.info("Partition: {} | Offset: {}", record.partition(), record.offset());
            }
            consumer.commitSync();
        }
    }
}

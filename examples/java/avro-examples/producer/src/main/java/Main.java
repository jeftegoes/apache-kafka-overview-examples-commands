import io.confluent.kafka.serializers.KafkaAvroSerializer;
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

    static void main() {
        System.setProperty(
                "org.apache.avro.SERIALIZABLE_CLASSES",
                "Customer"
        );

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");


        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties);
        String topic = "customer-avro-topic";

        Customer customer = Customer.newBuilder()
                .setAge(25)
                .setFirstName("Brenno")
                .setLastName("Salvador")
                .setHeight(100f)
                .setWeight(30.2f)
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);

        kafkaProducer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
            if (exception == null) {
                log.info("Sent successfully! Topic: {} | Partition: {} | Offset: {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                log.error("Error while producing message: {}", exception.getMessage());
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

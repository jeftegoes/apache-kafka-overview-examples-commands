package com.example;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ProducerSpringBootApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerSpringBootApplication.class, args);
    }

    @Bean
    public static CommandLineRunner commandLineRunner(KafkaProducer kafkaProducer) {
        return runner -> {
            Customer customer = new Customer("Bob",
                    "Smith",
                    10,
                    10f,
                    10f,
                    true);

            kafkaProducer.createCustomer(customer);
        };
    }
}

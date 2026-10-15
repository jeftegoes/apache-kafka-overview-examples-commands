package com.example;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class CosnumerSpringBootApplication {

    public static void main(String[] args) {
        SpringApplication.run(CosnumerSpringBootApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner() {
        return runner -> {
            System.out.println("Consumer Spring Boot Application is running...");
        };
    }
}

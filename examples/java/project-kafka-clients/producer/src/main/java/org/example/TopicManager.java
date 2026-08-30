package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicManager {
    private static final Logger log = LoggerFactory.getLogger(MainKeys.class);

    public static void createTopicIfNotExists(String bootstrapServers, String topicName, int partitions, short replicationFactor) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(config)) {
            boolean topicExists = adminClient.listTopics().names().get().contains(topicName);

            if (!topicExists) {
                NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                log.info("Topic '{}' created with {} partitions.", topicName, partitions);
            } else {
                log.info("Topic '{}' already exists.", topicName);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to create topic: {}", e.getMessage());
        }
    }
}
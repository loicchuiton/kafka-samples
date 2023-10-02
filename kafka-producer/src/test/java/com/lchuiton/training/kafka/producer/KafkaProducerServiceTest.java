package com.lchuiton.training.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
@SpringBootTest
class KafkaProducerServiceTest {

    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.5"))
        .waitingFor(Wait.forLogMessage(".*Container confluentinc/cp-kafka:7.3.5 started.*\\n", 1));

    @Autowired
    KafkaProducerService kafkaProducerService;

    @Autowired
    KafkaConsumer<String, String> kafkaConsumer;

    @DynamicPropertySource
    static void initEnv(DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVER", () -> kafkaContainer.getBootstrapServers());
    }

    @Test
    void messageShouldBeReceivedAndProcessed() {
        // Arrange
        String expectedMessagePayload = "Hello World! And Mom and Dad";
        kafkaConsumer.subscribe(Collections.singleton("eventTest.tpc"));

        // Act
        kafkaProducerService.sendMessage(expectedMessagePayload);

        // Assert
        Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) {
                return false;
            }
            for (ConsumerRecord<String, String> consumerRecord : records) {

                assertEquals(expectedMessagePayload, consumerRecord.value());
                Header version = consumerRecord.headers().lastHeader("version");
                assertEquals("1", new String(version.value()));

            }
            return true;
        });

    }

    @Configuration
    @Import(KafkaProducerConfig.class)
    static class TestConfig {

        @Bean
        KafkaConsumer<String, String> kafkaConsumer(KafkaProperties kafkaProperties) {
            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            return new KafkaConsumer<>(consumerProperties);
        }

        @Bean
        KafkaProducerService kafkaProducerService(KafkaProducer<String, String> kafkaProducer) {
            return new KafkaProducerService(kafkaProducer);
        }

    }

}
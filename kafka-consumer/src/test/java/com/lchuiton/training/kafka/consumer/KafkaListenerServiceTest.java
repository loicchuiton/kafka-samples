package com.lchuiton.training.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.verify;

@Testcontainers
@SpringBootTest
class KafkaListenerServiceTest {

    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
        .waitingFor(Wait.forLogMessage(".*Container confluentinc/cp-kafka:6.2.1 started.*\\n", 1));

    @Autowired
    KafkaListenerService kafkaListenerService;

    @Autowired
    KafkaProducer<String, String> kafkaProducer;

    @Autowired
    KafkaService kafkaService;

    @Captor
    ArgumentCaptor<ConsumerRecord<String, String>> argumentCaptor;

    @DynamicPropertySource
    static void initEnv(DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVER", () -> kafkaContainer.getBootstrapServers());
    }

    @Test
    void messageShouldBeReceivedAndProcessed() {
        // Arrange
        String expectedMessagePayload = "Hello World! And Mom and Dad";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("eventTest.tpc", "testcontainers",
                expectedMessagePayload);
        producerRecord.headers().add(new RecordHeader("version", "1".getBytes()));

        // Act
        try {
            kafkaProducer.send(producerRecord).get();
        }
        catch (ExecutionException | InterruptedException exception) {
            fail();
        }

        // Assert
        verify(kafkaService, Mockito.timeout(3000L)).processKafkaMessage(argumentCaptor.capture());

        ConsumerRecord<String, String> capturedConsumerRecord = argumentCaptor.getValue();
        Header version = capturedConsumerRecord.headers().lastHeader("version");
        assertEquals(expectedMessagePayload, capturedConsumerRecord.value());
        assertEquals("1", new String(version.value()));

    }

    @Configuration
    @EnableKafka
    @EnableAutoConfiguration
    static class TestConfig {

        @MockBean
        KafkaService kafkaService;

        @Bean
        KafkaListenerService kafkaListenerService(KafkaService kafkaService) {
            return new KafkaListenerService(kafkaService);
        }

        @Bean
        KafkaProducer<String, String> kafkaProducer() {
            return new KafkaProducer<>(
                    ImmutableMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                            ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()),
                    new StringSerializer(), new StringSerializer());
        }

    }

}
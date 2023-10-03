package com.lchuiton.training.kafka.reactive.consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
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
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Testcontainers
@SpringBootTest
class KafkaReactiveReceiverServiceTest {

    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.5"))
        .waitingFor(Wait.forLogMessage(".*Container confluentinc/cp-kafka:7.3.5 started.*\\n", 1));

    @Autowired
    KafkaReactiveReceiverService kafkaReactiveReceiverService;

    @Autowired
    KafkaProducer<String, String> kafkaProducer;

    @Autowired
    KafkaService kafkaService;

    @Captor
    ArgumentCaptor<ReceiverRecord<String, String>> argumentCaptor;

    @DynamicPropertySource
    static void initEnv(DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVER", () -> kafkaContainer.getBootstrapServers());
    }

    @Test
    void messageShouldBeReceivedAndProcessed() throws ExecutionException, InterruptedException {
        // Arrange
        String expectedMessagePayload = "Hello World! And Mom and Dad";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("eventTest.tpc",
                UUID.randomUUID().toString(), expectedMessagePayload);
        producerRecord.headers().add(new RecordHeader("version", "1".getBytes()));

        TestPublisher<RecordMetadata> testPublisher = TestPublisher.create();

        // Act - Assert
        StepVerifier.create(kafkaReactiveReceiverService.processReceiverRecords()).then(() -> {
            try {
                testPublisher.emit(kafkaProducer.send(producerRecord).get());
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        })
            .expectNextMatches(e -> expectedMessagePayload.equals(e.value())
                    && "1".equals(new String(e.headers().lastHeader("version").value())))
            .expectNoEvent(Duration.of(2L, ChronoUnit.SECONDS))
            .verifyTimeout(Duration.of(2L, ChronoUnit.SECONDS));
    }

    @Configuration
    @Import(KafkaReactiveReceiverConfig.class)
    static class TestConfig {

        @Bean
        KafkaService kafkaService() {
            return new KafkaService();
        }

        @Bean
        KafkaReactiveReceiverService kafkaListenerService(KafkaService kafkaService,
                KafkaReceiver<String, String> kafkaReceiver) {
            return new KafkaReactiveReceiverService(kafkaService, kafkaReceiver);
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
package com.lchuiton.training.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class KafkaListenerService {

    private final KafkaService kafkaService;

    Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    public KafkaListenerService(KafkaService kafkaService) {

        this.kafkaService = kafkaService;
    }

    @KafkaListener(topics = "eventTest.tpc")
    public void listen(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {

        String message = consumerRecord.value();
        int partition = consumerRecord.partition();
        long offset = consumerRecord.offset();

        logger.info("Receive message [{}] from partition {} - offset {}", message, partition, offset);
        logger.info("Headers");

        consumerRecord.headers()
            .forEach(e -> logger.info("## {}={}", e.key(), new String(e.value(), StandardCharsets.UTF_8)));

        kafkaService.processKafkaMessage(consumerRecord);

        acknowledgment.acknowledge();
    }

}

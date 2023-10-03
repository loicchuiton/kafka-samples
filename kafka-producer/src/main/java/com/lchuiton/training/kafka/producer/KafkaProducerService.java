package com.lchuiton.training.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.UUID.randomUUID;

@Service
public class KafkaProducerService implements ProducerService<String> {

    private final KafkaProducer<String, String> kafkaProducer;

    private final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    @Autowired
    public KafkaProducerService(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void sendMessage(String msg) {
        // Set custom headers
        Iterable<Header> messageHeaders = List.of(new RecordHeader("version", "1".getBytes()));

        // Create a new ProducerRecord
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("eventTest.tpc", null,
                java.time.Instant.now().getEpochSecond(), randomUUID().toString(), msg, messageHeaders);

        // Send ProducerRecord to Kafka
        kafkaProducer.send(producerRecord);
        logger.info("Message {} sent.", producerRecord.key());
    }

}

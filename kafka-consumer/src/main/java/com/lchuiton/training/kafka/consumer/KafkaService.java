package com.lchuiton.training.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

    Logger logger = LoggerFactory.getLogger(KafkaService.class);

    public void processKafkaMessage(ConsumerRecord<String, String> messageValue) {
        logger.info("Something to be done with Kafka message with value [{}]. ", messageValue.value());

    }

}

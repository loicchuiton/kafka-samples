package com.lchuiton.training.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@Service
public class KafkaService {

    Logger logger = LoggerFactory.getLogger(KafkaService.class);

    public Mono<ReceiverRecord<String, String>> processKafkaMessage(ReceiverRecord<String, String> messageValue) {
        logger.info("Something to be done with Kafka message with value [{}]. ", messageValue.value());
        return Mono.just(messageValue);
    }

}

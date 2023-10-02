package com.lchuiton.training.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ScheduledMessageSender {

    private final KafkaProducerService kafkaProducerService;

    private int iteration = 0;

    @Autowired
    public ScheduledMessageSender(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @Scheduled(initialDelay = 1000L, fixedRate = 10000L)
    public void producer() {
        kafkaProducerService.sendMessage("test " + iteration++);
    }

}

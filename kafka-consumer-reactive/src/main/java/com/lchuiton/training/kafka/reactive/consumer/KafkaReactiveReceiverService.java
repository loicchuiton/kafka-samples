package com.lchuiton.training.kafka.reactive.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;

import static reactor.util.retry.Retry.fixedDelay;

@Service
public class KafkaReactiveReceiverService {

    private final KafkaService kafkaService;

    private final KafkaReceiver<String, String> kafkaReceiver;

    Logger logger = LoggerFactory.getLogger(KafkaReactiveReceiverService.class);

    public KafkaReactiveReceiverService(KafkaService kafkaService, KafkaReceiver<String, String> kafkaReceiver) {
        this.kafkaService = kafkaService;
        this.kafkaReceiver = kafkaReceiver;
    }

    protected Flux<ReceiverRecord<String, String>> processReceiverRecords() {
        return kafkaReceiver.receive()
            .doOnError(error -> logger.error("Error receiving event, will retry", error))
            .retryWhen(fixedDelay(Long.MAX_VALUE, Duration.ofMinutes(1)))
            .doOnNext(receiverRecord -> logger.info("Received event: key {} - partition {} - offset {}",
                    receiverRecord.key(), receiverRecord.partition(), receiverRecord.offset()))
            .concatMap(kafkaService::processKafkaMessage);

    }

    // @EventListener(ApplicationStartedEvent.class)
    private Disposable eventListener() {
        return processReceiverRecords().subscribe(receiverRecord -> receiverRecord.receiverOffset().acknowledge());
    }

}

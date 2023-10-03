package com.lchuiton.training.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.HashMap;
import java.util.Map;

import static java.time.Duration.ofMillis;
import static java.util.List.of;
import static reactor.kafka.receiver.KafkaReceiver.create;

@Configuration
@EnableConfigurationProperties({KafkaProperties.class})
public class KafkaReactiveReceiverConfig {

    @Bean
    public Map<String, Object> kafkaConsumerProperties(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumer().getGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getConsumer().getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaProperties.getConsumer().getEnableAutoCommit());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return props;
    }

    protected ReceiverOptions<String, String> kafkaReceiverOptions(KafkaProperties kafkaProperties) {
        ReceiverOptions<String, String> options = ReceiverOptions.create(kafkaConsumerProperties(kafkaProperties));
        return options.pollTimeout(ofMillis(500L)).subscription(of("eventTest.tpc"));
    }

    @Bean
    KafkaReceiver<String, String> kafkaReceiver(KafkaProperties kafkaProperties) {
        return create(kafkaReceiverOptions(kafkaProperties));
    }

}
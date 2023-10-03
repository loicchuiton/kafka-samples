package com.lchuiton.training.kafka.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaSimpleProducer {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(KafkaSimpleProducer.class);
        application.setAdditionalProfiles("int");
        application.run(args);
    }

}

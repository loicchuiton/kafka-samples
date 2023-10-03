package com.lchuiton.training.kafka.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaSimpleListener {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(KafkaSimpleListener.class);
        application.setAdditionalProfiles("int");
        application.run(args);
    }

}

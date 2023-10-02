package com.lchuiton.training.kafka.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(TestApplication.class);
        application.setAdditionalProfiles("int");
        application.run(args);
    }

}

# Kafka Samples

This project contains a set of Producers and Consumers for Apache Kafka, using Spring Boot and Spring Cloud.

## Run locally

### Docker local environment

- Execute command `docker compose up` or launch from docker-compose.yml

The Docker environment provides a Zookeeper, a Kafka broker and another Kafka container to initialize a test topic with
a predefine number of partitions on the Kafka Broker.

### Start Spring Application

- Run `TestApplication.java` of one or more of the projects to publish or consume messages, depending of the type of
  project. Some customization is possible using `application-int.yml` files.

## Unit Testing with TestContainers

This project also demonstrate how to test an Apache Kafka integration using TestContainers. This helps to
understand the behaviour of producing and consuming messages through Kafka and test different fields of the
ConsumerRecords.  

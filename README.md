# Kafka Samples

This project contains a set of Producers and Consumers for Apache Kafka, using Spring Boot and Spring Cloud.

## Run locally

### Docker local environment

- Execute command `docker compose up` or launch from docker-compose.yml

The Docker environment provides a Zookeeper, a Kafka broker and another Kafka container to initialize a test topic with
a predefine number of partitions on the Kafka Broker.

### Start Spring Application

- Run one or more of the projects to publish and/or consume messages, depending on the type of
  project. Some customization is possible using `application-int.yml` files.

## Unit Testing with TestContainers

This project also demonstrate how to test an Apache Kafka integration using TestContainers. This helps to
understand the behaviour of producing and consuming messages through Kafka and test different fields of the
ConsumerRecords.

## TODOs

- [ ] Create an Avro Consumer
- [ ] Parametrize topic name in all configurations
- [ ] Shared KafkaService in a common module (reactive and non-reactive)
- [ ] Add a way to visualize consumption metrics
- [ ] More type of producers (volume, objects of different size, batching)
- [ ] Optimize StepVerifier for reactive tests
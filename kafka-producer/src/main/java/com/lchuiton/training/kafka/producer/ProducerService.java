package com.lchuiton.training.kafka.producer;

public interface ProducerService<T> {

    void sendMessage(T message);

}

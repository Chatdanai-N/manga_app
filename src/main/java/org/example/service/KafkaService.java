package org.example.service;

import org.example.repository.entity.MangaRequestData;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

public interface KafkaService {

    public void produceMessage(String key, String jsonPayload);

    public void consumeMessage(@Payload String message, @Header(KafkaHeaders.TOPIC) String topicName, @Header(KafkaHeaders.RECEIVED_KEY) String key, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition, @Header(KafkaHeaders.OFFSET) long offset, Acknowledgment ack);

    public String stopConsume();

    public String startConsume();

    public String getStatus();

    public MangaRequestData saveMangaRequestData(MangaRequestData mangaRequestData);

    public void resendMessage(String processStatus);

}

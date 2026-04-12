package org.example.service.impl;

import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.example.repository.MangaRawDataRepository;
import org.example.repository.MangaRequestDataRepository;
import org.example.repository.entity.MangaRawData;
import org.example.repository.entity.MangaRequestData;
import org.example.service.KafkaService;
import org.example.utils.DateUtils;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Objects;


@Service
@Slf4j
public class KafkaServiceImpl implements KafkaService {


    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MangaRawDataRepository mangaRawDataRepository;
    private final MangaRequestDataRepository mangaRequestDataRepository;
    private final KafkaListenerEndpointRegistry registry;

    public KafkaServiceImpl(KafkaTemplate<String, String> kafkaTemplate, MangaRawDataRepository mangaRawDataRepository, MangaRequestDataRepository mangaRequestDataRepository, KafkaListenerEndpointRegistry registry) {
        this.kafkaTemplate = kafkaTemplate;
        this.mangaRawDataRepository = mangaRawDataRepository;
        this.mangaRequestDataRepository = mangaRequestDataRepository;
        this.registry = registry;
    }


    @Override
    public void produceMessage(String key, String jsonPayload) {
        try {
            String topic = "push.manga";

            // insert manga request
            MangaRequestData mangaRequestData = new MangaRequestData();
            mangaRequestData.setKeyValue(key);
            mangaRequestData.setPayload(jsonPayload);
            mangaRequestData.setTopicName(topic);
            mangaRequestData.setProcessStatus("N");
            mangaRequestData.setCreatedDate(DateUtils.convertToLocalDateTime(new Date()));

            MangaRequestData savedMangaRequestData = this.saveMangaRequestData(mangaRequestData);

            if(Objects.nonNull(savedMangaRequestData.getId())){
                kafkaTemplate.send(topic, key, jsonPayload)
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                handleSuccess(savedMangaRequestData.getId(), jsonPayload, result.getRecordMetadata().offset());
                            } else {
                                handleFailure(savedMangaRequestData.getId(), jsonPayload, ex);
                            }
                        });
            }


        } catch (Exception e) {
            log.error("process produceMessage key: {} payload: {} error: {}", key, jsonPayload,e.getMessage());
        }
    }


    @Override
    @KafkaListener(topics = "${spring.kafka.topic.name}", id = "manga-listener-id", autoStartup = "true")
    @Transactional
    public void consumeMessage(@Payload String payload, @Header(KafkaHeaders.RECEIVED_TOPIC) String topicName,
                               @Header(KafkaHeaders.RECEIVED_KEY) String key,
                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partitionId,
                               @Header(KafkaHeaders.OFFSET) long offset, Acknowledgment ack) {

        try {
            MangaRawData mangaRawData = new MangaRawData();
            mangaRawData.setTopicName(topicName);
            mangaRawData.setPartitionId(partitionId);
            mangaRawData.setOffsetVal(offset);
            mangaRawData.setKeyValue(key);
            mangaRawData.setPayload(payload);
            mangaRawData.setCreatedDate(DateUtils.convertToLocalDateTime(new Date()));

            mangaRawDataRepository.save(mangaRawData);
            ack.acknowledge();
            log.info("Message at offset {} acknowledged successfully", offset);
        } catch (Exception ex) {
            log.error("Failed to save message at offset {}: {}", offset, ex.getMessage());
        }

    }

    @Override
    public String stopConsume() {
        MessageListenerContainer container = registry.getListenerContainer("manga-listener-id");
        if (container != null && container.isRunning()) {
            container.stop(); // หรือใช้ container.pause() ถ้าไม่อยากตัดการเชื่อมต่อ
            return "Stopped: Kafka consumer is now OFF";
        }
        return "Consumer is already stopped or not found";
    }

    @Override
    public String startConsume() {
        MessageListenerContainer container = registry.getListenerContainer("manga-listener-id");
        if (container != null && !container.isRunning()) {
            container.start(); // หรือใช้ container.resume()
            return "Started: Kafka consumer is now ON";
        }
        return "Consumer is already running";
    }

    @Override
    public String getStatus() {
        MessageListenerContainer container = registry.getListenerContainer("manga-listener-id");
        if (container == null) {
            return "ERROR: Consumer with ID 'manga-consumer-id' not found!";
        }

        return container.isRunning() ? "RUNNING (ON)" : "STOPPED (OFF)";
    }

    @Override
    @Transactional
    public MangaRequestData saveMangaRequestData(MangaRequestData mangaRequestData) {
        return mangaRequestDataRepository.save(mangaRequestData);
    }

    @Override
    public void resendMessage(String processStatus) {


        List<MangaRequestData> mangaRequestDataList = mangaRequestDataRepository.searchByProcessStatus(processStatus);

        for (MangaRequestData mangaRequestData : mangaRequestDataList) {

            long id = mangaRequestData.getId();
            String topicName = mangaRequestData.getTopicName();
            String key = mangaRequestData.getKeyValue();
            String jsonPayload = mangaRequestData.getPayload();

            kafkaTemplate.send(topicName, key, jsonPayload)
                    .whenComplete((result, ex) -> {
                                if (ex == null) {
                                    handleSuccess(id, jsonPayload, result.getRecordMetadata().offset());
                                } else {
                                    handleFailure(id, jsonPayload, ex);
                                }
                    } );


        }
    }

    private void handleSuccess(long id, String payload, long offset){
        log.info("Sent message: [{}] with offset: [{}]", payload, offset);
        updateStatus(id, "Y");
    }

    private void handleFailure(long id, String payload, Throwable ex) {
        log.error("Unable to send message: [{}] due to: {}", payload, ex.getMessage());
        updateStatus(id, "F");
    }

    @Transactional
    private void updateStatus(long id, String status){
        try{
            int rows = mangaRequestDataRepository.updateProcessStatus(status, LocalDateTime.now(), id);
            log.info("Update status {} for ID: {} (rows affected: {})", status, id, rows);
        } catch (Exception e) {
            log.error("Failed to update DB for ID: {}", id, e);
        }
    }

}

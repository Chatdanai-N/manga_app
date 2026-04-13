package com.example.service.impl;

import org.apache.kafka.clients.producer.RecordMetadata;
import com.example.repository.MangaRawDataRepository;
import com.example.repository.MangaRequestDataRepository;
import com.example.repository.entity.MangaRawData;
import com.example.repository.entity.MangaRequestData;
import com.example.service.MangaSystemCacheService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaServiceTest {

    @Mock
    private MangaRequestDataRepository mangaRequestDataRepository;

    @Mock
    private MangaRawDataRepository mangaRawDataRepository;

    @Mock
    private KafkaTemplate<String , String> kafkaTemplate;

    @Mock
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Mock
    private MangaSystemCacheService mangaSystemCacheService;

    @InjectMocks
    private KafkaServiceImpl kafkaService;

    private MangaRequestData createMockMangaRequestData(Long id) {
        MangaRequestData data = new MangaRequestData();
        data.setId(id);
        data.setTopicName("manga-topic");
        data.setKeyValue("key-" + id);
        data.setPayload("{\"title\":\"One Piece\"}");
        return data;
    }

    private MangaRawData createMockMangaRawData(String topicName, int partitionId, long offset, String key, String payload){
        MangaRawData mangaRawData = new MangaRawData();

        mangaRawData.setTopicName(topicName);
        mangaRawData.setPartitionId(partitionId);
        mangaRawData.setOffsetVal(offset);
        mangaRawData.setKeyValue(key);
        mangaRawData.setPayload(payload);
        mangaRawData.setCreatedDate(LocalDateTime.now());
        return mangaRawData;
    }

    @Test
    void should_update_status_to_Y_when_produce_message_success(){

        // Arrange
        String mangaId = Long.valueOf(1L).toString();
        String payload = "{\n" +
                "  \"artist\" : \"Kei Urana\",\n" +
                "  \"author\" : \"Kei Urana\",\n" +
                "  \"demographic\" : \"Shonen\",\n" +
                "  \"genres\" : [ \"Action\", \"Dark Fantasy\", \"Dystopian\" ],\n" +
                "  \"manga_id\" : \"MNG-GAK-001\",\n" +
                "  \"publisher\" : {\n" +
                "    \"th\" : \"Kodansha (Thai License)\",\n" +
                "    \"jp\" : \"Kodansha (Weekly Shonen Magazine)\"\n" +
                "  },\n" +
                "  \"rating\" : 4.7,\n" +
                "  \"specialFeatures\" : null,\n" +
                "  \"status\" : \"ONGOING\",\n" +
                "  \"synopsis\" : \"รูโด้ เด็กหนุ่มจากย่านสลัมบนฟากฟ้า ถูกใส่ร้ายในคดีฆาตกรรมและถูกโยนลงไปยัง 'นรก' เบื้องล่างที่เป็นที่ทิ้งขยะ เขาต้องเอาชีวิตรอดและล้างแค้นด้วยพลังจากถุงมือคู่ใจ\",\n" +
                "  \"systemData\" : null,\n" +
                "  \"tags\" : [ \"Trash\", \"Soul Power\", \"Revenge\", \"Graffiti Art Style\" ],\n" +
                "  \"title\" : {\n" +
                "    \"th\" : \"กาลีอักคุตะ\",\n" +
                "    \"en\" : \"Gachiakuta\",\n" +
                "    \"jp\" : \"ガチアクタ\"\n" +
                "  },\n" +
                "  \"updated_at\" : \"2026-03-28T17:20:00Z\",\n" +
                "  \"volumes\" : { },\n" +
                "  \"worldSetting\" : null\n" +
                "}";

        String topic = "manga-topic";
        given(mangaSystemCacheService.getKafkaParameterByCode(anyString())).willReturn(topic);

        MangaRequestData mockData = createMockMangaRequestData(1L);
        given(kafkaService.saveMangaRequestData(any())).willReturn(mockData);


        CompletableFuture<SendResult<String,String>> future = new CompletableFuture<>();
        SendResult<String, String> sendResult = mock(SendResult.class);
        RecordMetadata metadata = new RecordMetadata(null, 0, 0, 0, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(metadata);
        future.complete(sendResult);

        given(kafkaTemplate.send(anyString(),anyString(),anyString())).willReturn(future);


        //Act (When)
        kafkaService.produceMessage(mangaId,payload);


        //Assert (Then)
        then(mangaRequestDataRepository).should(timeout(1000)).updateProcessStatus(eq("Y"), any(), eq(1L));
    }

    @Test
    void should_update_status_to_F_when_produce_message_fails(){
        // Arrange
        String mangaId = Long.valueOf(1L).toString();
        String payload = "{\n" +
                "  \"artist\" : \"Kei Urana\",\n" +
                "  \"author\" : \"Kei Urana\",\n" +
                "  \"demographic\" : \"Shonen\",\n" +
                "  \"genres\" : [ \"Action\", \"Dark Fantasy\", \"Dystopian\" ],\n" +
                "  \"manga_id\" : \"MNG-GAK-001\",\n" +
                "  \"publisher\" : {\n" +
                "    \"th\" : \"Kodansha (Thai License)\",\n" +
                "    \"jp\" : \"Kodansha (Weekly Shonen Magazine)\"\n" +
                "  },\n" +
                "  \"rating\" : 4.7,\n" +
                "  \"specialFeatures\" : null,\n" +
                "  \"status\" : \"ONGOING\",\n" +
                "  \"synopsis\" : \"รูโด้ เด็กหนุ่มจากย่านสลัมบนฟากฟ้า ถูกใส่ร้ายในคดีฆาตกรรมและถูกโยนลงไปยัง 'นรก' เบื้องล่างที่เป็นที่ทิ้งขยะ เขาต้องเอาชีวิตรอดและล้างแค้นด้วยพลังจากถุงมือคู่ใจ\",\n" +
                "  \"systemData\" : null,\n" +
                "  \"tags\" : [ \"Trash\", \"Soul Power\", \"Revenge\", \"Graffiti Art Style\" ],\n" +
                "  \"title\" : {\n" +
                "    \"th\" : \"กาลีอักคุตะ\",\n" +
                "    \"en\" : \"Gachiakuta\",\n" +
                "    \"jp\" : \"ガチアクタ\"\n" +
                "  },\n" +
                "  \"updated_at\" : \"2026-03-28T17:20:00Z\",\n" +
                "  \"volumes\" : { },\n" +
                "  \"worldSetting\" : null\n" +
                "}";

        String topic = "manga-topic";
        given(mangaSystemCacheService.getKafkaParameterByCode(anyString())).willReturn(topic);

        MangaRequestData mockData = createMockMangaRequestData(1L);
        given(kafkaService.saveMangaRequestData(any())).willReturn(mockData);

        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Kafka connection failed!"));
        given(kafkaTemplate.send(anyString(),anyString(),anyString())).willReturn(future);

        //Act (When)
        kafkaService.produceMessage(mangaId,payload);


        //Assert (Then)
        then(mangaRequestDataRepository).should(timeout(1000)).updateProcessStatus(eq("F"), any(), eq(1L));
    }


    @Test
    void should_raise_exception_when_manga_request_data_fails(){
        // Arrange
        String mangaId = Long.valueOf(1L).toString();
        String payload = "{\n" +
                "  \"artist\" : \"Kei Urana\",\n" +
                "  \"author\" : \"Kei Urana\",\n" +
                "  \"demographic\" : \"Shonen\",\n" +
                "  \"genres\" : [ \"Action\", \"Dark Fantasy\", \"Dystopian\" ],\n" +
                "  \"manga_id\" : \"MNG-GAK-001\",\n" +
                "  \"publisher\" : {\n" +
                "    \"th\" : \"Kodansha (Thai License)\",\n" +
                "    \"jp\" : \"Kodansha (Weekly Shonen Magazine)\"\n" +
                "  },\n" +
                "  \"rating\" : 4.7,\n" +
                "  \"specialFeatures\" : null,\n" +
                "  \"status\" : \"ONGOING\",\n" +
                "  \"synopsis\" : \"รูโด้ เด็กหนุ่มจากย่านสลัมบนฟากฟ้า ถูกใส่ร้ายในคดีฆาตกรรมและถูกโยนลงไปยัง 'นรก' เบื้องล่างที่เป็นที่ทิ้งขยะ เขาต้องเอาชีวิตรอดและล้างแค้นด้วยพลังจากถุงมือคู่ใจ\",\n" +
                "  \"systemData\" : null,\n" +
                "  \"tags\" : [ \"Trash\", \"Soul Power\", \"Revenge\", \"Graffiti Art Style\" ],\n" +
                "  \"title\" : {\n" +
                "    \"th\" : \"กาลีอักคุตะ\",\n" +
                "    \"en\" : \"Gachiakuta\",\n" +
                "    \"jp\" : \"ガチアクタ\"\n" +
                "  },\n" +
                "  \"updated_at\" : \"2026-03-28T17:20:00Z\",\n" +
                "  \"volumes\" : { },\n" +
                "  \"worldSetting\" : null\n" +
                "}";

        String topic = "manga-topic";
        given(mangaSystemCacheService.getKafkaParameterByCode(anyString())).willReturn(topic);

        given(kafkaService.saveMangaRequestData(any())).willThrow(new RuntimeException("Database connection failed"));

        //act (When)
        kafkaService.produceMessage(mangaId,payload);

        //Assert
        then(kafkaTemplate).shouldHaveNoInteractions();

    }




    @Test
    void should_update_process_status_to_Y_when_kafka_resend_success(){

        // Arrange
        MangaRequestData mockData = createMockMangaRequestData(1L);
        given(mangaRequestDataRepository.searchByProcessStatus("F")).willReturn(List.of(mockData));

        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        SendResult<String, String> sendResult = mock(SendResult.class);
        RecordMetadata metadata = new RecordMetadata(null, 0, 0, 0, 0, 0);

        given(sendResult.getRecordMetadata()).willReturn(metadata);
        future.complete(sendResult);

        given(kafkaTemplate.send(anyString(),anyString(),anyString())).willReturn(future);

        // Act (When)
        kafkaService.resendMessage("F");

        // Assert (Then)
        then(mangaRequestDataRepository).should(timeout(1000)).updateProcessStatus(eq("Y"), any(), eq(1L));

    }

    @Test
    void should_update_process_status_to_F_when_kafka_resend_fail(){

        //Arrange
        MangaRequestData mockData = createMockMangaRequestData(2L);
        given(mangaRequestDataRepository.searchByProcessStatus("F")).willReturn(List.of(mockData));

        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Kafka connection failed!"));

        given(kafkaTemplate.send(anyString(),anyString(),anyString())).willReturn(future);

        // Act (When)
        kafkaService.resendMessage("F");

        // Assert (Then)
        then(mangaRequestDataRepository).should(timeout(1000)).updateProcessStatus(eq("F"), any(), eq(2L));

    }




    @Test
    void should_save_manga_raw_data_when_consume_success(){

        //Arrange
        String payload = "{\n" +
                "  \"artist\" : \"Kei Urana\",\n" +
                "  \"author\" : \"Kei Urana\",\n" +
                "  \"demographic\" : \"Shonen\",\n" +
                "  \"genres\" : [ \"Action\", \"Dark Fantasy\", \"Dystopian\" ],\n" +
                "  \"manga_id\" : \"MNG-GAK-001\",\n" +
                "  \"publisher\" : {\n" +
                "    \"th\" : \"Kodansha (Thai License)\",\n" +
                "    \"jp\" : \"Kodansha (Weekly Shonen Magazine)\"\n" +
                "  },\n" +
                "  \"rating\" : 4.7,\n" +
                "  \"specialFeatures\" : null,\n" +
                "  \"status\" : \"ONGOING\",\n" +
                "  \"synopsis\" : \"รูโด้ เด็กหนุ่มจากย่านสลัมบนฟากฟ้า ถูกใส่ร้ายในคดีฆาตกรรมและถูกโยนลงไปยัง 'นรก' เบื้องล่างที่เป็นที่ทิ้งขยะ เขาต้องเอาชีวิตรอดและล้างแค้นด้วยพลังจากถุงมือคู่ใจ\",\n" +
                "  \"systemData\" : null,\n" +
                "  \"tags\" : [ \"Trash\", \"Soul Power\", \"Revenge\", \"Graffiti Art Style\" ],\n" +
                "  \"title\" : {\n" +
                "    \"th\" : \"กาลีอักคุตะ\",\n" +
                "    \"en\" : \"Gachiakuta\",\n" +
                "    \"jp\" : \"ガチアクタ\"\n" +
                "  },\n" +
                "  \"updated_at\" : \"2026-03-28T17:20:00Z\",\n" +
                "  \"volumes\" : { },\n" +
                "  \"worldSetting\" : null\n" +
                "}";

        String topicName = "push.topic";
        String key = "MNG-GAK-001";
        int partitionId = 1;
        long offset = 1L;
        Acknowledgment ack = mock(Acknowledgment.class);

        MangaRawData mangaRawData = this.createMockMangaRawData(topicName,partitionId,offset,key,payload);
        given(mangaRawDataRepository.save(any())).willReturn(mangaRawData);

        // Act (When)
        kafkaService.consumeMessage(payload,topicName,key,partitionId,offset,ack);

        // Assert
        then(ack).should().acknowledge();
    }

    @Test
    void should_raise_exception_when_save_manga_raw_data_fails(){

        //Arrange
        String payload = "{\n" +
                "  \"artist\" : \"Kei Urana\",\n" +
                "  \"author\" : \"Kei Urana\",\n" +
                "  \"demographic\" : \"Shonen\",\n" +
                "  \"genres\" : [ \"Action\", \"Dark Fantasy\", \"Dystopian\" ],\n" +
                "  \"manga_id\" : \"MNG-GAK-001\",\n" +
                "  \"publisher\" : {\n" +
                "    \"th\" : \"Kodansha (Thai License)\",\n" +
                "    \"jp\" : \"Kodansha (Weekly Shonen Magazine)\"\n" +
                "  },\n" +
                "  \"rating\" : 4.7,\n" +
                "  \"specialFeatures\" : null,\n" +
                "  \"status\" : \"ONGOING\",\n" +
                "  \"synopsis\" : \"รูโด้ เด็กหนุ่มจากย่านสลัมบนฟากฟ้า ถูกใส่ร้ายในคดีฆาตกรรมและถูกโยนลงไปยัง 'นรก' เบื้องล่างที่เป็นที่ทิ้งขยะ เขาต้องเอาชีวิตรอดและล้างแค้นด้วยพลังจากถุงมือคู่ใจ\",\n" +
                "  \"systemData\" : null,\n" +
                "  \"tags\" : [ \"Trash\", \"Soul Power\", \"Revenge\", \"Graffiti Art Style\" ],\n" +
                "  \"title\" : {\n" +
                "    \"th\" : \"กาลีอักคุตะ\",\n" +
                "    \"en\" : \"Gachiakuta\",\n" +
                "    \"jp\" : \"ガチアクタ\"\n" +
                "  },\n" +
                "  \"updated_at\" : \"2026-03-28T17:20:00Z\",\n" +
                "  \"volumes\" : { },\n" +
                "  \"worldSetting\" : null\n" +
                "}";

        String topicName = "push.topic";
        String key = "MNG-GAK-001";
        int partitionId = 1;
        long offset = 1L;
        Acknowledgment ack = mock(Acknowledgment.class);

        given(mangaRawDataRepository.save(any())).willThrow(new RuntimeException("Database connection failed!"));

        // Act (When)
        kafkaService.consumeMessage(payload,topicName,key,partitionId,offset,ack);

        // Assert (Then)
        then(ack).shouldHaveNoInteractions();

    }


    @Test
    void should_stop_container_consumer_kafka_when_container_is_running(){

        //Arrange
        MessageListenerContainer messageListenerContainer = mock(MessageListenerContainer.class);
        messageListenerContainer.setupMessageListener("manga-listener-id");


        given(kafkaListenerEndpointRegistry.getListenerContainer("manga-listener-id")).willReturn(messageListenerContainer);
        given(messageListenerContainer.isRunning()).willReturn(true);


        // Act (When)
        String result = kafkaService.stopConsume();


        // Assert (Then)
        assertEquals("Stopped: Kafka consumer is now OFF", result);
        then(messageListenerContainer).should(times(1)).stop();
    }

    @Test
    void should_do_nothing_when_when_container_is_not_running(){

        //Arrange
        MessageListenerContainer messageListenerContainer = mock(MessageListenerContainer.class);
        messageListenerContainer.setupMessageListener("manga-listener-id");

        given(kafkaListenerEndpointRegistry.getListenerContainer("manga-listener-id")).willReturn(messageListenerContainer);
        given(messageListenerContainer.isRunning()).willReturn(false);


        // Act (When)
        String result = kafkaService.stopConsume();

        // Assert (then)
        assertEquals("Consumer is already stopped or not found", result);
        then(messageListenerContainer).should(never()).stop();
    }

    @Test
    void should_start_container_consume_kafka_when_container_is_not_running(){

        //Arrange
        MessageListenerContainer messageListenerContainer = mock(MessageListenerContainer.class);
        messageListenerContainer.setupMessageListener("manga-listener-id");

        given(kafkaListenerEndpointRegistry.getListenerContainer("manga-listener-id")).willReturn(messageListenerContainer);
        given(messageListenerContainer.isRunning()).willReturn(false);


        // Act (When)
        String result = kafkaService.startConsume();

        // Assert (then)
        assertEquals("Started: Kafka consumer is now ON", result);
        then(messageListenerContainer).should(times(1)).start();

    }

    @Test
    void should_do_nothing_container_when_container_is_running(){

        //Arrange
        MessageListenerContainer messageListenerContainer = mock(MessageListenerContainer.class);
        messageListenerContainer.setupMessageListener("manga-listener-id");

        given(kafkaListenerEndpointRegistry.getListenerContainer("manga-listener-id")).willReturn(messageListenerContainer);
        given(messageListenerContainer.isRunning()).willReturn(true);

        // Act (When)
        String result = kafkaService.startConsume();


        // Assert (then)
        assertEquals("Consumer is already running", result);
        then(messageListenerContainer).should(never()).start();

    }

    @Test
    void should_return_status_running_when_container_is_running(){

        //Arrange
        MessageListenerContainer messageListenerContainer = mock(MessageListenerContainer.class);
        messageListenerContainer.setupMessageListener("manga-listener-id");

        given(kafkaListenerEndpointRegistry.getListenerContainer("manga-listener-id")).willReturn(messageListenerContainer);
        given(messageListenerContainer.isRunning()).willReturn(true);

        // Act (When)
        String result = kafkaService.getStatus();

        // Assert (then)
        assertEquals("RUNNING (ON)", result);


    }

    @Test
    void should_return_status_stopping_when_container_is_not_running(){

        //Arrange
        MessageListenerContainer messageListenerContainer = mock(MessageListenerContainer.class);
        messageListenerContainer.setupMessageListener("manga-listener-id");

        given(kafkaListenerEndpointRegistry.getListenerContainer("manga-listener-id")).willReturn(messageListenerContainer);
        given(messageListenerContainer.isRunning()).willReturn(false);

        // Act (When)
        String result = kafkaService.getStatus();

        // Assert (then)
        assertEquals("STOPPED (OFF)", result);

    }

    @Test
    void should_raise_exception_when_update_status_fail(){

        // Arrange
        String mangaId = Long.valueOf(1L).toString();
        String payload = "{\n" +
                "  \"artist\" : \"Kei Urana\",\n" +
                "  \"author\" : \"Kei Urana\",\n" +
                "  \"demographic\" : \"Shonen\",\n" +
                "  \"genres\" : [ \"Action\", \"Dark Fantasy\", \"Dystopian\" ],\n" +
                "  \"manga_id\" : \"MNG-GAK-001\",\n" +
                "  \"publisher\" : {\n" +
                "    \"th\" : \"Kodansha (Thai License)\",\n" +
                "    \"jp\" : \"Kodansha (Weekly Shonen Magazine)\"\n" +
                "  },\n" +
                "  \"rating\" : 4.7,\n" +
                "  \"specialFeatures\" : null,\n" +
                "  \"status\" : \"ONGOING\",\n" +
                "  \"synopsis\" : \"รูโด้ เด็กหนุ่มจากย่านสลัมบนฟากฟ้า ถูกใส่ร้ายในคดีฆาตกรรมและถูกโยนลงไปยัง 'นรก' เบื้องล่างที่เป็นที่ทิ้งขยะ เขาต้องเอาชีวิตรอดและล้างแค้นด้วยพลังจากถุงมือคู่ใจ\",\n" +
                "  \"systemData\" : null,\n" +
                "  \"tags\" : [ \"Trash\", \"Soul Power\", \"Revenge\", \"Graffiti Art Style\" ],\n" +
                "  \"title\" : {\n" +
                "    \"th\" : \"กาลีอักคุตะ\",\n" +
                "    \"en\" : \"Gachiakuta\",\n" +
                "    \"jp\" : \"ガチアクタ\"\n" +
                "  },\n" +
                "  \"updated_at\" : \"2026-03-28T17:20:00Z\",\n" +
                "  \"volumes\" : { },\n" +
                "  \"worldSetting\" : null\n" +
                "}";

        String topic = "manga-topic";
        given(mangaSystemCacheService.getKafkaParameterByCode(anyString())).willReturn(topic);

        MangaRequestData mockData = createMockMangaRequestData(1L);
        given(kafkaService.saveMangaRequestData(any())).willReturn(mockData);


        CompletableFuture<SendResult<String,String>> future = new CompletableFuture<>();
        SendResult<String, String> sendResult = mock(SendResult.class);
        RecordMetadata metadata = new RecordMetadata(null, 0, 0, 0, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(metadata);
        future.complete(sendResult);


        given((mangaRequestDataRepository.updateProcessStatus(anyString(), any(), anyLong()))).willThrow(new RuntimeException("Database connection failed"));

        given(kafkaTemplate.send(anyString(),anyString(),anyString())).willReturn(future);


        //Act (When)
        kafkaService.produceMessage(mangaId,payload);


        then(mangaRequestDataRepository).should(timeout(1000)) .updateProcessStatus(eq("Y"), any(), eq(1L));

    }
}

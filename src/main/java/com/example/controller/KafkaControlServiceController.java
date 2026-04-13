package com.example.controller;

import lombok.Generated;
import com.example.service.KafkaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Generated
@RequestMapping("/api/kafka")
public class KafkaControlServiceController {

    private final KafkaService kafkaService;

    public KafkaControlServiceController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @GetMapping("/stop")
    public ResponseEntity<String> stop() {
        return ResponseEntity.ok(kafkaService.stopConsume());
    }

    @GetMapping("/start")
    public ResponseEntity<String> start() {
        return ResponseEntity.ok(kafkaService.startConsume());
    }

    @GetMapping("/status")
    public ResponseEntity<String> checkStatus() {
        String status = kafkaService.getStatus();
        return ResponseEntity.ok(status);
    }

}

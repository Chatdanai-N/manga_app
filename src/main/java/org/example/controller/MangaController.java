package org.example.controller;


import lombok.Generated;
import org.example.bean.MangaRequest;
import org.example.service.KafkaService;
import org.springframework.data.repository.query.Param;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.ObjectMapper;

@RestController
@Generated
@RequestMapping("/api/manga")
public class MangaController {

    private final KafkaService kafkaService;

    public MangaController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @PostMapping("/send")
    public String sendMessage(@RequestBody MangaRequest mangaRequest) {

        String mangaId = mangaRequest.getMangaId();
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonPayload = objectMapper.writeValueAsString(mangaRequest);

        kafkaService.produceMessage(mangaId,jsonPayload);

        return "Started";
    }

    @PostMapping("/resend")
    public String resendMessage(@Param("processStatus") String processStatus ){

        kafkaService.resendMessage(processStatus);

        return "Success";
    }

}

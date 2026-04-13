package com.example.bean;


import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class MangaRequest {

    @NotBlank(message = "Manga ID is required")
    @JsonProperty("manga_id")
    private String mangaId;

    @NotEmpty(message = "Title cannot be empty")
    private Map<String, String> title; // รองรับ th, en, jp, kr

    private String author;
    private String artist;

    private Map<String, String> publisher;
    private String status; // ONGOING, COMPLETED

    private List<String> genres;
    private String demographic; // Shonen, Seinen

    private Volume volumes;

    private SystemData systemData; // สำหรับ Solo Leveling (พวก Rank, Class, Summons)

    private Map<String, String> worldSetting; // สำหรับ Gachiakuta (พวก Location, Power System)

    private SpecialFeature specialFeatures; // สำหรับ Dandadan (พวก Anime Studio)

    private Double rating;
    private String synopsis;
    private List<String> tags;

    @JsonProperty("updated_at")
    private String updatedAt;
}

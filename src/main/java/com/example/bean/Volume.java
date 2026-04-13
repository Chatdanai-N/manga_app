package com.example.bean;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.Data;

import java.util.List;

@Data
public class Volume {

    @Min(value = 0, message = "Total released volumes cannot be negative")
    private Integer totalReleased;

    @PositiveOrZero(message = "Total chapters must be 0 or more")
    private Integer totalChapters;

    private List<Integer> ownedVolumes;

    @Max(value = 2000, message = "Last read chapter seems too high")
    private Integer lastReadChapter;
}

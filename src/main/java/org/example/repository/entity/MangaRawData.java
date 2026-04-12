package org.example.repository.entity;


import jakarta.persistence.*;
import lombok.Data;


import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "manga_raw_data")
public class MangaRawData {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    @Column(name = "TOPIC_NAME", nullable = false)
    private String topicName;
    @Column(name = "PARTITION_ID")
    private Integer partitionId;
    @Column(name = "OFFSET_VAL")
    private long  offsetVal;
    @Column(name = "payload", columnDefinition = "TEXT")
    private String payload;
    @Column(name = "KEY_VALUE")
    private String keyValue;
    @Column(name = "CREATED_DATE")
    private LocalDateTime createdDate;
}

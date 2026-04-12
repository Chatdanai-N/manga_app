package org.example.repository.entity;


import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "manga_request_data")
public class MangaRequestData {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    @Column(name = "TOPIC_NAME", nullable = false)
    private String topicName;
    @Column(name = "PAYLOAD", columnDefinition = "TEXT")
    private String payload;
    @Column(name = "KEY_VALUE")
    private String keyValue;
    @Column(name = "PROCESS_STATUS")
    private String processStatus;
    @Column(name = "CREATED_DATE")
    private LocalDateTime createdDate;
    @Column(name = "UPDATED_DATE")
    private LocalDateTime updatedDate;
}

package com.example.repository;

import jakarta.transaction.Transactional;
import com.example.repository.entity.MangaRequestData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface MangaRequestDataRepository extends JpaRepository<MangaRequestData,Long> {

        @Query("SELECT m FROM MangaRequestData m WHERE m.id = :id")
        MangaRequestData searchById(@Param("id") long id);

        @Query("SELECT m FROM MangaRequestData m WHERE m.processStatus = :processStatus")
        List<MangaRequestData> searchByProcessStatus(@Param("processStatus") String processStatus);

        @Modifying
        @Transactional
        @Query("UPDATE MangaRequestData m set m.processStatus = :processStatus, m.updatedDate = :updatedDate where m.id = :id ")
        int updateProcessStatus(String processStatus, LocalDateTime updatedDate, long id);


}

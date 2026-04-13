package com.example.repository;

import com.example.repository.entity.MangaRawData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MangaRawDataRepository extends JpaRepository<MangaRawData,Long> {

}

package com.example.repository;

import com.example.repository.entity.MangaSystemParameter;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public interface MangaSystemParameterRepository extends JpaRepository<MangaSystemParameter,Long> {


    List<MangaSystemParameter> findAllByOrderByParameterIdAsc();

}

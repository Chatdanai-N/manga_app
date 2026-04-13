package com.example.service.impl;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import com.example.repository.MangaSystemParameterRepository;
import com.example.repository.entity.MangaSystemParameter;
import com.example.service.MangaSystemCacheService;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Service
public class MangaSystemCacheServiceImpl implements MangaSystemCacheService {


    private final MangaSystemParameterRepository mangaSystemParameterRepository;
    private final CacheManager cacheManager;


    public MangaSystemCacheServiceImpl(MangaSystemParameterRepository mangaSystemParameterRepository, CacheManager cacheManager) {
        this.mangaSystemParameterRepository = mangaSystemParameterRepository;
        this.cacheManager = cacheManager;
    }


    @Override
    @PostConstruct
    public void init() {

        int maxRetries = 3;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                this.preloadDataToCache();
                log.info("Preload cache success on attempt {}", attempt);
                return;
            } catch (Exception e) {
                log.error("Attempt {} failed: {}", attempt, e.getMessage());

                if (attempt < maxRetries) {
                    delay(); // แยก method หน่วงเวลาเพื่อความสะอาด
                } else {
                    log.error("Preload cache failed after {} attempts. Giving up.", maxRetries);
                    throw new RuntimeException("Application failed to start: Cache preloading failed.", e);
                }
            }
        }
    }

    private void delay() {
        try {
            Thread.sleep((long) 2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Retry delay interrupted", e);
        }
    }

    @Override
    public void preloadDataToCache() {

        List<MangaSystemParameter> mangaSystemParameterList = mangaSystemParameterRepository.findAllByOrderByParameterIdAsc();

        Cache kafkaCache = cacheManager.getCache("KAFKA_CONFIG");
        log.info("Initial Cache KAFKA_CONFIG.");
        if (Objects.nonNull(kafkaCache)) {

            for (MangaSystemParameter mangaSystemParameter : mangaSystemParameterList) {

                if (mangaSystemParameter.getGroupCode().equals("KAFKA_CONFIG")) {
                    log.info("KAFKA_CONFIG, Code:{}, value:{}", mangaSystemParameter.getCode(), mangaSystemParameter.getValue() );
                    kafkaCache.put(mangaSystemParameter.getCode(), mangaSystemParameter.getValue());
                }
            }

        }
        log.info("Load Cache KAFKA_CONFIG SUCCESS.");

    }

    @Override
    public String getKafkaParameterByCode(String code) {

        return Optional.ofNullable(cacheManager.getCache("KAFKA_CONFIG"))
                .map(cache -> cache.get(code, String.class))
                .orElse(null);
    }


}

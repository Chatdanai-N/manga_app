package com.example.service;

public interface MangaSystemCacheService {

    public void init();

    public void preloadDataToCache();

    public String getKafkaParameterByCode(String code);

}

package com.example.service.impl;


import com.example.repository.MangaSystemParameterRepository;
import com.example.repository.entity.MangaSystemParameter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
public class MangaSystemCacheServiceTest {

    @Mock
    MangaSystemParameterRepository mangaSystemParameterRepository;

    @Mock
    CacheManager cacheManager;

    @InjectMocks
    MangaSystemCacheServiceImpl mangaSystemCacheService;


    @Test
    void should_init_cache_success_when_load_data_on_system_cache() {


        // Arrange
        MangaSystemParameter mangaSystemParameter = new MangaSystemParameter();
        mangaSystemParameter.setParameterId(1L);
        mangaSystemParameter.setGroupCode("KAFKA_CONFIG");
        mangaSystemParameter.setCode("TOPIC");
        mangaSystemParameter.setValue("mock.topic");
        mangaSystemParameter.setOrderNumber(1);

        List<MangaSystemParameter> mangaSystemParameterList = new ArrayList<>();
        mangaSystemParameterList.add(mangaSystemParameter);

        given(mangaSystemParameterRepository.findAllByOrderByParameterIdAsc()).willReturn(mangaSystemParameterList);

        Cache mockCache = mock(Cache.class);
        given(cacheManager.getCache(anyString())).willReturn(mockCache);


        // Act (When)
        mangaSystemCacheService.init();

        // Assert (Then)
        then(mangaSystemParameterRepository).should(times(1)).findAllByOrderByParameterIdAsc();
        then(cacheManager).should(times(1)).getCache(anyString());

    }

    @Test
    void should_retry_3time_and_raise_exception_when_load_data_on_raise_exception() {
        // Arrange
        given(mangaSystemParameterRepository.findAllByOrderByParameterIdAsc()).willThrow(new RuntimeException("Connection Database Error"));

        Cache mangaCache = mock(Cache.class);

        // Act (When)
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            mangaSystemCacheService.init();
        });

        // Assert (Then)
        assertThat(exception.getMessage()).isEqualTo("Application failed to start: Cache preloading failed.");

        then(mangaSystemParameterRepository).should(times(3)).findAllByOrderByParameterIdAsc();
        then(cacheManager).should(never()).getCache(anyString());

    }

    @Test
    void should_return_parameter_string_when_call_method_get_kafka_parameter_by_code(){

        //Arrange
        String code = "mock-code";
        String expectedValue = "mock-values";

        Cache mockCache = mock(Cache.class);
        given(cacheManager.getCache(anyString())).willReturn(mockCache);

        given(mockCache.get(code,String.class )).willReturn(expectedValue);

        //Act (When)
        String result = mangaSystemCacheService.getKafkaParameterByCode(code);

        //Assert (Then)
        assertThat(result).isEqualTo(expectedValue);
        then(cacheManager).should(times(1)).getCache(anyString());
        then(mockCache).should().get(code,String.class);

    }

    @Test
    void should_return_null_when_call_method_get_kafka_parameter_by_code(){

        //Arrange
        String code = "mock_code";

        Cache mockCache = mock(Cache.class);
        given(cacheManager.getCache(anyString())).willReturn(null);

        //Act (When)
        String result = mangaSystemCacheService.getKafkaParameterByCode(code);


        //Assert (Then)
        assertThat(result).isNull();
        then(cacheManager).should().getCache(anyString());
    }
}

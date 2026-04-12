package org.example;

import lombok.Generated;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@Generated
@EnableKafka
class MangaApplication {

    public static void main(String[] args) {
        SpringApplication.run(MangaApplication.class, args);
    }

}

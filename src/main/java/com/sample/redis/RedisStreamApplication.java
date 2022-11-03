package com.sample.redis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RedisStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisStreamApplication.class, args);
    }

}

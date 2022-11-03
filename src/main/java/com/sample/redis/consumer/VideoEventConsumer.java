package com.sample.redis.consumer;


import com.sample.redis.model.VideoDetails;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class VideoEventConsumer implements StreamListener<String, ObjectRecord<String, VideoDetails>> {

    private AtomicInteger atomicInteger = new AtomicInteger(0);

    @Autowired
    private ReactiveRedisTemplate<String, String> redisTemplate;

    @Override
    @SneakyThrows
    public void onMessage(ObjectRecord<String, VideoDetails> record) {
        log.info(InetAddress.getLocalHost().getHostName() + " - consumed :" + record.getValue());
        if (record.getValue().getLikes()) {
            this.redisTemplate
                    .opsForZSet()
                    .incrementScore(record.getValue().getVideo().getName(), "Likes", 1)
                    .subscribe();
        }
        if (record.getValue().getDisLike()) {
            this.redisTemplate
                    .opsForZSet()
                    .incrementScore(record.getValue().getVideo().getName(), "Dislikes", 1)
                    .subscribe();
        }
        this.redisTemplate
                .opsForZSet()
                .incrementScore(record.getValue().getVideo().getName(), "Views", 1)
                .subscribe();
        this.redisTemplate
                .opsForZSet()
                .incrementScore(record.getValue().getVideo().getName(), "Rating", record.getValue().getRating())
                .subscribe();
        atomicInteger.incrementAndGet();
    }

    @Scheduled(fixedRate = 10000)
    public void showPublishedEventsSoFar() {
        log.info("Total Consumer :: " + atomicInteger.get());
    }

}

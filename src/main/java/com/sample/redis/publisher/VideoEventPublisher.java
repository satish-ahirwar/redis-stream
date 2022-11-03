package com.sample.redis.publisher;

import com.sample.redis.model.VideoDetails;
import com.sample.redis.repo.VideoRepository;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class VideoEventPublisher {

    private AtomicInteger atomicInteger = new AtomicInteger(0);

    @Value("${stream.key:video-streams}")
    private String streamKey;

    private final VideoRepository videoRepository;

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    public VideoEventPublisher(VideoRepository repository,
                               ReactiveRedisTemplate<String, String> redisTemplate) {
        this.videoRepository = repository;
        this.redisTemplate = redisTemplate;
    }

    @Scheduled(fixedRateString = "${publish.rate}")
    public void publishEvent() {
        VideoDetails VideoDetails = this.videoRepository.getRandomVideo();
        log.info("Video Details :: " + VideoDetails);
        ObjectRecord<String, VideoDetails> record = StreamRecords.newRecord()
                .ofObject(VideoDetails)
                .withStreamKey(streamKey);
        this.redisTemplate
                .opsForStream()
                .add(record)
                .subscribe(System.out::println);
        atomicInteger.incrementAndGet();
    }

    @Scheduled(fixedRate = 10000)
    public void showPublishedEventsSoFar() {
        log.info("Total Events :: " + atomicInteger.get());
    }

}

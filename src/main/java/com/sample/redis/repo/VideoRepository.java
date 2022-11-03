package com.sample.redis.repo;


import com.sample.redis.model.Video;
import com.sample.redis.model.VideoDetails;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Repository
public class VideoRepository {

    public static final List<Video> video_LIST = Stream.of(
            new Video(1, "Avengers End Game", "Marvel Studios"),
            new Video(2, "Avengers Infinity War", "Marvel Studios"),
            new Video(3, "Dark Knight", "Warner Bros"),
            new Video(4, "Pulp Fiction", "MiraMax"),
            new Video(5, "Fight Club", "Warner Bros"),
            new Video(6, "Good Fellas", "Warner Bros"),
            new Video(7, "Seven", "Warner Bros"),
            new Video(8, "Cast Away", "ImageMovers Playtone"),
            new Video(9, "Forest Gump", "The Tisch Company"),
            new Video(10, "King Kong", "Warner Bros"),
            new Video(11, "The Silence Of Lambs", "Strong Heart Productions"),
            new Video(12, "Usual Suspects", "PolyGram Filmed Entertainment"),
            new Video(13, "Green Mile", "Castle Rock Entertainment"),
            new Video(14, "No Country For Old Men", "Scott Rudin Productions"),
            new Video(15, "Train to Busan", "Next Entertainment World"),
            new Video(16, "Parasite", "Barunson E&A"),
            new Video(17, "Whiplash", "Sony Pictures"),
            new Video(18, "The Prestige", "Warner Bros"),
            new Video(19, "Joker", "Warner Bros"),
            new Video(20, "Old Boy", "Show East"),
            new Video(21, "I Saw Devil", "Peppermint and company"),
            new Video(22, "The Perfect Murder", "Warner Bros"),
            new Video(23, "The Chaser", "Snow Box"),
            new Video(24, "Goodwill Hunting", "Be Gentlemen"),
            new Video(25, "Snatch", "Columbia Pictures")
    ).collect(Collectors.toList());

    public VideoDetails getRandomVideo() {
        Integer index = ThreadLocalRandom.current().nextInt(0, 25);
        Video video = video_LIST.get(index);
        Random random = new Random();
        Integer value = random.ints(0, 1000).findFirst().getAsInt();
        Double rating = random.doubles(1.0, 10.0).findFirst().getAsDouble();
        return new VideoDetails(video, value % 2 == 0, value % 2 == 1, rating);
    }


}

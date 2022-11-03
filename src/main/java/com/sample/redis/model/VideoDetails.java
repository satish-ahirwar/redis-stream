package com.sample.redis.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VideoDetails {
    private Video video;
    private Boolean likes = false;
    private Boolean disLike = false;
    private Double rating;
}

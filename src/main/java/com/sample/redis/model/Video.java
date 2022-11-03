package com.sample.redis.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Video {
    private Integer id;
    private String name;
    private String author;
}

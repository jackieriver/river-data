package com.river.flink.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Score {

    private Integer id;

    private String name;

    private String item;

    private Integer score;

    private String tag;

    private LocalDateTime localDateTime;
}

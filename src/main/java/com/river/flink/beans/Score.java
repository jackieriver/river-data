package com.river.flink.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Score {

    private Integer id;

    private String name;

    private String item;

    private Integer score;

    private String tag;
}

package com.river.flink.Demo.window.time;

import cn.hutool.json.JSONUtil;
import com.river.flink.beans.Score;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.activemq.internal.RunningChecker;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.time.ZoneOffset;

/**
 * @author JackieRiver
 * @Title:
 * @Package
 * @Description: 时间窗口函数
 * @date 2021-01-07下午 11:20
 */

@Slf4j
public class TimeWindowDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.getConfig().setAutoWatermarkInterval(100);
        RunningChecker runningChecker = new RunningChecker();
        runningChecker.setIsRunning(true);
        RMQConnectionConfig rmqConnectionConfig = new RMQConnectionConfig.Builder()
                .setHost("192.168.0.128")
                .setPort(5672)
                .setUserName("guest")
                .setPassword("guest")
                .setVirtualHost("/")
                .build();

        DataStream dataStream = environment.addSource(new RMQSource<String>(rmqConnectionConfig, "dataStream",true, new SimpleStringSchema()));
        dataStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return JSONUtil.toBean(value, Score.class).getId();
            }
        }).print();
        dataStream.map(new MapFunction<String, Score>() {
            @Override
            public Score map(String o) throws Exception {
                Score score = JSONUtil.toBean(o, Score.class);
                log.info(score.getId() + " === "+ score.getLocalDateTime().toString());
                return score;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Score>() {
            @Override
            public long extractAscendingTimestamp(Score element) {
                return element.getLocalDateTime().toInstant(ZoneOffset.of("+8")).toEpochMilli();
            }
        })
                //.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)))

        .keyBy(new KeySelector<Score, String>() {
            @Override
            public String getKey(Score o) throws Exception {
                return o.getTag();
            }
        })
                .timeWindow(Time.seconds(5), Time.seconds(5))
                .allowedLateness(Time.seconds(0))
        .maxBy("score").print();







        environment.execute();
    }
}

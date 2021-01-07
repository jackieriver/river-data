package com.river.flink.Demo.window.time;

import cn.hutool.json.JSONUtil;
import com.river.flink.Demo.mq.DataStreamRMQProductor;
import com.river.flink.beans.Score;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.activemq.internal.RunningChecker;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

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

        dataStream.map(new MapFunction<String, Score>() {
            @Override
            public Score map(String o) throws Exception {
                return JSONUtil.toBean(o, Score.class);
            }
        }).keyBy(new KeySelector<Score, String>() {
            @Override
            public String getKey(Score o) throws Exception {
                return o.getName();
            }
        })
                //滚动时间窗口
                .timeWindow(Time.seconds(20)).apply(new WindowFunction<Score, Score, String, TimeWindow>() {

            @Override
            public void apply(String s, TimeWindow window, Iterable<Score> input, Collector<Score> out) throws Exception {
                if (input.iterator().hasNext()){
                    out.collect(input.iterator().next());
                }
                log.info("{} - {}", s);
            }
        }).print();



        environment.execute();
    }
}

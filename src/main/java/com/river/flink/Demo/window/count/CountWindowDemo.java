package com.river.flink.Demo.window.count;

import cn.hutool.json.JSONUtil;
import com.river.flink.beans.Score;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.activemq.internal.RunningChecker;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * @author JackieRiver
 * @Title:
 * @Package
 * @Description: �������ں���
 * @date 2021-01-07���� 11:20
 */

@Slf4j
public class CountWindowDemo {

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
                //1. ���� ��������, ���ڴ�С,��������
                .countWindow(5, 5).maxBy("score")
                //2. �������ں���
                //.countWindow(5).maxBy("score")


        .print();



        environment.execute();
    }
}

package com.river.flink.Demo.mq;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.activemq.internal.RunningChecker;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;


/**
 * @description:
 * @author: He Pengfei
 * @time: 2021/1/7 9:49
 */
public class DataStreamRMQConsumer {

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

        dataStream.print();

        environment.execute();
    }
}

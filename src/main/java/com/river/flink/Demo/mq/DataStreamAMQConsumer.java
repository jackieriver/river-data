package com.river.flink.Demo.mq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.activemq.AMQSource;
import org.apache.flink.streaming.connectors.activemq.AMQSourceConfig;
import org.apache.flink.streaming.connectors.activemq.internal.RunningChecker;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * @description:
 * @author: He Pengfei
 * @time: 2021/1/7 9:49
 */
public class DataStreamAMQConsumer {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        AMQSourceConfig amqSourceConfig = new AMQSourceConfig.AMQSourceConfigBuilder()
                .setConnectionFactory(new ActiveMQConnectionFactory("admin", "admin", "tcp://mq.testgfxd.com:61616"))
                .setDestinationName("dataStream")
                .setDeserializationSchema(new SimpleStringSchema())
                .setRunningChecker(new RunningChecker())
                .build();
        DataStream dataStream = environment.addSource(new AMQSource(amqSourceConfig));
        dataStream.print();
        environment.execute();
    }
}

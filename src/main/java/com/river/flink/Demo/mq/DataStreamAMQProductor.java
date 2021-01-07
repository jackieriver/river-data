package com.river.flink.Demo.mq;

import cn.hutool.json.JSONUtil;
import com.river.flink.beans.Score;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.activemq.*;
import org.apache.flink.streaming.connectors.activemq.internal.RunningChecker;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

/**
 * @description:
 * @author: He Pengfei
 * @time: 2021/1/7 9:49
 */
public class DataStreamAMQProductor {


    private final static List<String> items = new ArrayList();
    private final static List<String> names = new ArrayList();

    static {
        items.add("����");
        items.add("��ѧ");
        items.add("Ӣ��");
        items.add("����");

        names.add("����");
        names.add("���");
        names.add("��ٻ");
        names.add("����");
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        //��������
        DataStream<Score> source = environment.addSource(new SourceFunction<Score>() {


            @Override
            public void run(SourceContext<Score> sourceContext) throws Exception {
                Random random = new Random();
                AtomicInteger atomicInteger = new AtomicInteger(0);
                while (true){
                    names.stream().forEach(name -> {
                        items.stream().forEach(item -> {
                            try {
                                sleep(1000);
                            } catch (InterruptedException e) {

                            }
                            sourceContext.collect(new Score(atomicInteger.incrementAndGet(), name, item, random.nextInt(100), "mq"));
                        });
                    });
                }
            }

            @Override
            public void cancel() {

            }
        });
        source.print();

        //mq����
        AMQSinkConfig amqSinkConfig = new AMQSinkConfig(new ActiveMQConnectionFactory("admin", "admin", "tcp://mq.testgfxd.com:61616"),
                "dataStream",
                new SimpleStringSchema(),
                false,
                DestinationType.QUEUE);
        AMQSink amqSink = new AMQSink(amqSinkConfig);

        //д����mq
        SingleOutputStreamOperator<String> outputStreamOperator = source.map(new MapFunction<Score, String>() {
            @Override
            public String map(Score score) throws Exception {

                return JSONUtil.toJsonStr(score);
            }
        });

        outputStreamOperator.addSink(amqSink);

        environment.execute();
    }
}

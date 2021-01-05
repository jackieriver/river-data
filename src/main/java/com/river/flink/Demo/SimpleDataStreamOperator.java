package com.river.flink.Demo;

import com.river.flink.beans.Score;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleDataStreamOperator {

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
        DataStream<Score> source = environment.addSource(new SourceFunction<Score>() {


            @Override
            public void run(SourceContext<Score> sourceContext) throws Exception {
                Random random = new Random();
                AtomicInteger atomicInteger = new AtomicInteger(0);
                names.stream().forEach(name -> {
                    items.stream().forEach(item -> {
                        sourceContext.collect(new Score(atomicInteger.incrementAndGet(), name, item, random.nextInt(100)));
                    });
                });
            }

            @Override
            public void cancel() {

            }
        });
        source.print();
        /*source = source.map(new MapFunction<Score, Score>() {
            @Override
            public Score map(Score score) throws Exception {
                //score.setScore(score.getScore() * 10);
                return score;
            }
        });*/

        KeyedStream<Score, String> scoreStringKeyedStream = source.keyBy(new KeySelector<Score, String>() {
            @Override
            public String getKey(Score score) throws Exception {
                return score.getName();
            }
        });
        //scoreStringKeyedStream.print();
        /*scoreStringKeyedStream.reduce(new ReduceFunction<Score>() {
            @Override
            public Score reduce(Score score, Score t1) throws Exception {
                Score score1 = new Score();
                score1.setScore(score.getScore() + t1.getScore());
                score1.setName(score.getName());
                score1.setItem("�ܷ���");
                return score1;
            }
        }).print();*/
        System.out.println("=============================================================================");
        //scoreStringKeyedStream.max("score").print();
        scoreStringKeyedStream.maxBy("score").print();
        scoreStringKeyedStream.sum("score").print();

        environment.execute();
    }
}

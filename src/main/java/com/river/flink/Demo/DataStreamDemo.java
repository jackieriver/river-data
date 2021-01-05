package com.river.flink.Demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class DataStreamDemo {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //数据生成
        DataStream<String> dataStreamDemoSource = environment.addSource(new SourceFunction<String>() {

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                for(int i=0; i<=10; i++){
                    sourceContext.collect(randomWord());
                }
            }

            @Override
            public void cancel() {

            }
        });

        dataStreamDemoSource.print();
        dataStreamDemoSource =  dataStreamDemoSource.map(new MapFunction<String, String>() {


            @Override
            public String map(String o) throws Exception {
                return o.toUpperCase();
            }
        });
        dataStreamDemoSource.print();

        dataStreamDemoSource = dataStreamDemoSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                collector.collect(s.substring(0, s.length()/2));
                collector.collect(s.substring(s.length()/2));
            }
        });
        dataStreamDemoSource.print();

        environment.execute();
    }


    private static String randomWord() {
        int length = 12 + (int) (Math.random() * 9);
        String word = "";
        for (int i = 0; i < length; i++) {
            word += (char) randomChar();
        }
        return word;
    }

    private static byte randomChar() {
        int flag = (int) (Math.random() * 2);// 0小写字母1大写字母
        byte resultBt;
        if (flag == 0) {
            byte bt = (byte) (Math.random() * 26);// 0 <= bt < 26
            resultBt = (byte) (65 + bt);
        } else {
            byte bt = (byte) (Math.random() * 26);// 0 <= bt < 26
            resultBt = (byte) (97 + bt);
        }
        return resultBt;
    }

}

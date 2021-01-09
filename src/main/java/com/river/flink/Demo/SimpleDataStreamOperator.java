package com.river.flink.Demo;

import com.river.flink.beans.Score;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

public class SimpleDataStreamOperator {

    private final static List<String> items = new ArrayList();
    private final static List<String> names = new ArrayList();

    static {
        items.add("语文");
        items.add("数学");
        items.add("英语");
        items.add("物理");

        names.add("泽文");
        names.add("向刚");
        names.add("高倩");
        names.add("春考");
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        //生产数据
        DataStream<Score> source = environment.addSource(new SourceFunction<Score>() {


            @Override
            public void run(SourceContext<Score> sourceContext) throws Exception {
                Random random = new Random();
                AtomicInteger atomicInteger = new AtomicInteger(0);
                names.stream().forEach(name -> {
                    items.stream().forEach(item -> {
                        try {
                            sleep(1000);
                        } catch (InterruptedException e) {

                        }
                        sourceContext.collectWithTimestamp(new Score(atomicInteger.incrementAndGet(), name, item, random.nextInt(100), "local", LocalDateTime.now()), System.currentTimeMillis());
                    });
                });
            }

            @Override
            public void cancel() {

            }
        });
        source.print();
        source.keyBy(new KeySelector<Score, String>() {
            @Override
            public String getKey(Score value) throws Exception {
                return value.getName();
            }
        }).maxBy("score").print();

        /*AMQSourceConfig amqSourceConfig = new AMQSourceConfig.AMQSourceConfigBuilder()
                .setConnectionFactory(new ActiveMQConnectionFactory("admin", "admin", "tcp://mq.testgfxd.com:61616"))
                .setDestinationName("dataStream")
                .setDeserializationSchema(new SimpleStringSchema())
                .setRunningChecker(new RunningChecker())
                .build();
        DataStream source_mq = environment.addSource(new AMQSource(amqSourceConfig));

        source.connect(source_mq).flatMap(new CoFlatMapFunction<Score, String, Score>() {

            @Override
            public void flatMap1(Score value, Collector<Score> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void flatMap2(String value, Collector<Score> out) throws Exception {
                out.collect(JSONUtil.toBean(value, Score.class));
            }
        }).print();*/
/*
        SingleOutputStreamOperator<Score> process = source.process(new ProcessFunction<Score, Score>() {
            @Override
            public void processElement(Score value, Context ctx, Collector<Score> out) throws Exception {
                if (value.getName().equals("高倩")) {
                    out.collect(value);
                    System.out.println(ctx.timestamp());
                }
            }
        });*/
        /*process.print();
        List<PojoField> fields = new ArrayList<>();
        List<PojoField> collect =
                Arrays.stream(Score.class.getFields()).map(field -> new PojoField(field, BasicTypeInfo.of(field.getDeclaringClass()))).collect(Collectors.toList());
        fields.addAll(collect);
        TypeInformation<Score> scoreTypeInformation = new PojoTypeInfo<Score>(Score.class, fields);
        process.getSideOutput(new OutputTag<>("1", scoreTypeInformation)).print();
*/
        environment.execute();
    }
}

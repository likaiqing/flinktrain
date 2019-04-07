package com.imooc.flink.java.wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @author: likaiqing
 * @create: 2019-04-05 22:08
 **/
public class WikipediaAnalysis {
    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent value) throws Exception {
                return value.getUser();
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedEdits.timeWindow(Time.seconds(5))
                .fold(new Tuple2<String, Long>("", 0l), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> accumulator, WikipediaEditEvent value) throws Exception {
                        accumulator.f0 = value.getUser();
                        accumulator.f1 += value.getByteDiff();
                        return accumulator;
                    }
                });
//        keyedEdits.timeWindow(Time.seconds(5))
//                .reduce(new ReduceFunction<WikipediaEditEvent>() {
//                    @Override
//                    public WikipediaEditEvent reduce(WikipediaEditEvent value1, WikipediaEditEvent value2) throws Exception {
//                        //TODO 设置新的byte值
//                        return value1;
//                    }
//                });
        result.print();
        result.map(s -> s.toString())
                .addSink(new FlinkKafkaProducer011<String>("localhost:9092", "flink_test", new SimpleStringSchema()));

        env.execute();

    }
}

package com.imooc.flink.java.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: likaiqing
 * @create: 2019-04-07 11:23
 **/
public class StreamingWCJavaApp {
    public static void main(String[] args) throws Exception {
        int port = 0;
        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println("端口未设置，使用默认端口9999");
            port = 9999;
        }

        //1,env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2,read
        DataStreamSource<String> text = env.socketTextStream("localhost", port);

        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String s : value.toLowerCase().split("\\s")) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        }).keyBy(0);//批处理是groupby,stream处理是keyby
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyBy.timeWindow(Time.seconds(5));
//        keyBy.
        window.sum(1).print();//.setParallelism(1);
        env.execute();
    }
}

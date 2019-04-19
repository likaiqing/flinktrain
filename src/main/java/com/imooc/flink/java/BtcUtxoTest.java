package com.imooc.flink.java;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class BtcUtxoTest {
    public static void main(String[] args) throws Exception {
        String bootStrapServers = "localhost:9092";
        String topic = "btx_utxo";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootStrapServers);
        properties.setProperty("group.id", "kqtest");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(consumer);
        source.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String value) throws Exception {
                String[] split = value.split("\t");
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long timeU = format.parse(split[0]).getTime() / 1000l;
                return new Tuple3<String, String, Long>(split[0], split[1], timeU);
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>>() {
            //超时时间
            Long maxOutOrderness = 5000l;

            //flink过一段时间便会调一次该函数获取水印
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis() - maxOutOrderness);
            }

            //每条记录多会调用 来获得even time 在生产的数据中 even_time放在字符串的第一个字段 用空格分割
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
                return element.f2;
            }
        }).keyBy(1).timeWindow(Time.seconds(300)).process(new ProcessWindowFunction<Tuple3<String, String, Long>, Object, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Object> out) throws Exception {

            }
        });

        env.execute();
    }
}

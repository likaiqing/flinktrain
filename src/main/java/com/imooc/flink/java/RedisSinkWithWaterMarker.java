package com.imooc.flink.java;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class RedisSinkWithWaterMarker {
    static final long maxOutOrderness = 3000l;

    public static void main(String[] args) throws Exception {
//        String bootStrapServers = "47.75.220.195:9092,47.75.12.110:9092,47.52.247.233:9092";
        String bootStrapServers = "localhost:9092";
//        String topic = "btx_utxo";
        String topic = "btx_utxo_test2";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///Users/likaiqing/space/okcoin/flinktrain/savepoint"));

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootStrapServers);
        properties.setProperty("group.id", "kqtest3");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        consumer.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> source = env.addSource(consumer);

        SingleOutputStreamOperator<Tuple2<Long, Integer>> map = source.map(str -> {
            String[] split = str.split("\t");
            SimpleDateFormat f1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return new Tuple2<Long, Integer>(f1.parse(split[0]).getTime(), 1);
        }).returns(Types.TUPLE(Types.LONG, Types.INT));

        SingleOutputStreamOperator<Tuple2<Long, Integer>> as = map.assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks());

        SingleOutputStreamOperator<Tuple2<Long, Integer>> result = as.map(t -> {
            return new Tuple2<Long, Integer>(t.f0 / 1000, t.f1);
        })
                .returns(Types.TUPLE(Types.LONG, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5), Time.seconds(2))
                .reduce((v1, v2) -> {
                    return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                }).returns(Types.TUPLE(Types.LONG, Types.INT));
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        result.addSink(new RedisSink<Tuple2<Long, Integer>>(conf, new MySetRedisMapper()));
        env.execute();
    }

    static class MySetRedisMapper implements RedisMapper<Tuple2<Long, Integer>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            System.out.println("RedisCommandDescription");
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(Tuple2<Long, Integer> data) {
            System.out.println("key:" + data.f0);
            return data.f0 + "";
        }

        @Override
        public String getValueFromData(Tuple2<Long, Integer> data) {
            System.out.println("value:" + data.f1);
            return data.f1 + "";
        }
    }

    static class MyAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<Long, Integer>> {
        //超时时间
//        Long maxOutOrderness = 3000l;

        long curMaxTimeStamp;

        //flink过一段时间便会调一次该函数获取水印
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
//            return new Watermark(System.currentTimeMillis() - maxOutOrderness);
            return new Watermark(curMaxTimeStamp - maxOutOrderness);
        }

        //每条记录多会调用 来获得even time 在生产的数据中 even_time放在字符串的第一个字段 用空格分割
        @Override
        public long extractTimestamp(Tuple2<Long, Integer> element, long previousElementTimestamp) {
            curMaxTimeStamp = Math.max(element.f0, curMaxTimeStamp);
            return element.f0;
        }
    }

    //        String op = "";
//        String flag = "";//认为标识
//        String opFieldBy = "";//
//        String groupId = "";//
//        try {
//            ParameterTool tool = ParameterTool.fromArgs(args);
//            bootStrapServers = tool.get("bootstrap.servers", bootStrapServers);
//            if (!tool.has("topic")) {//|| !tool.has("op")
//                throw new Exception("Usage:--bootstrap.servers ... --topic ... --group.id ... --op [incr|desc] --op_field_by ... ");
//            }
//            topic = tool.get("topic");
//            op = tool.get("op");
//            opFieldBy = tool.get("op_field_by");
//            groupId = tool.get("group.id");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }


//           source.print().setParallelism(1);
//    ObjectMapper mapper = new ObjectMapper();
//        source.flatMap(new FlatMapFunction<String, JsonBeanTest.RedisSinkWithWaterMarker>() {
//            @Override
//            public void flatMap(String json, Collector<JsonBeanTest.RedisSinkWithWaterMarker> out) throws Exception {
//                ObjectMapper mapper = new ObjectMapper();
//                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//                mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
//                JsonNode node = mapper.readTree(json);
//                JsonNode data = node.get("data");
//                if (data.isArray()) {
////                    Iterator<JsonNode> it = data.iterator();
////                    while (it.hasNext()) {
////                        JsonBeanTest.RedisSinkWithWaterMarker btcUtxo = mapper.convertValue(it.next(), JsonBeanTest.RedisSinkWithWaterMarker.class);
////                        System.out.println(btcUtxo);
////                    }
//                    JavaType beanList = mapper.getTypeFactory().constructParametricType(ArrayList.class, JsonBeanTest.RedisSinkWithWaterMarker.class);
//                    ArrayList<JsonBeanTest.RedisSinkWithWaterMarker> list = mapper.convertValue(data, beanList);
//                    list.stream().forEach(b -> out.collect(b));
//                }
//            }
//        }).keyBy("address").sum(1).map(new MapFunction<JsonBeanTest.RedisSinkWithWaterMarker, Tuple2<String,Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(JsonBeanTest.RedisSinkWithWaterMarker value) throws Exception {
//                return null;
//            }
//        });
}

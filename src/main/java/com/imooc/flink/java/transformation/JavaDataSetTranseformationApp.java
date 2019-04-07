package com.imooc.flink.java.transformation;

import com.imooc.flink.scala.transformation.DBUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: likaiqing
 * @create: 2019-04-07 16:17
 **/
public class JavaDataSetTranseformationApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        filterFunction(env);
//        mapPartitionFunction(env);
//        firstNFunction(env);
//        flatMapFunction(env);
//        distinctFunction(env);
//        joinFunction(env);
        crossFunction(env);
    }

    private static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> info1 = Arrays.asList("曼联", "曼城");
        List<Integer> info2 = Arrays.asList(3, 1, 0);
        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<Integer> data2 = env.fromCollection(info2);
        data1.cross(data2).print();
    }

    private static void joinFunction(ExecutionEnvironment env) throws Exception {
        ArrayList<Tuple2<Integer, String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1, "PK哥"));
        info1.add(new Tuple2<>(2, "J哥"));
        info1.add(new Tuple2<>(3, "小队长"));
        info1.add(new Tuple2<>(4, "猪头呼"));
        ArrayList<Tuple2<Integer, String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1, "北京"));
        info2.add(new Tuple2<>(2, "上海"));
        info2.add(new Tuple2<>(3, "成都"));
        info2.add(new Tuple2<>(5, "杭州"));
        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);
        //1,join
        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {

                return new Tuple3<>(first.f0, first.f1, second.f1);
            }
        }).print();
        //2,leftjoin
        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (second == null) {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
                } else {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }

            }
        }).print();
        //3,rightjoin
        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
                } else {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }

            }
        }).print();
        //4,fullouterjoin
        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
                } else if (second == null) {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
                } else {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }

            }
        }).sortPartition(0, Order.DESCENDING).print();
    }

    private static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");
        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.toLowerCase().split(",")) {
                    out.collect(s);
                }
            }
        }).distinct().print();
    }

    private static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");
        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.toLowerCase().split(",")) {
                    out.collect(s);
                }
            }
        })
                .map(s -> new Tuple2(s, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();
    }

    private static void firstNFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add("Student:" + i);
        }
        ArrayList<Tuple2<Integer, String>> info = new ArrayList<>();
        info.add(new Tuple2<Integer, String>(1, "Hadoop"));
        info.add(new Tuple2<Integer, String>(1, "Spark"));
        info.add(new Tuple2<Integer, String>(1, "Flink"));
        info.add(new Tuple2<Integer, String>(2, "Java"));
        info.add(new Tuple2<Integer, String>(2, "Spring Boot"));
        info.add(new Tuple2<Integer, String>(3, "Linux"));
        info.add(new Tuple2<Integer, String>(4, "VUE"));
        DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);
        data.first(3).print();
        System.out.println("--------------");
        data.groupBy(0).first(2).print();
        System.out.println("--------------");
        data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
    }

    private static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add("Student:" + i);
        }
        DataSource<String> data = env.fromCollection(list).setParallelism(3);
        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                String connection = DBUtils.getConnection();
                System.out.println("connect:[" + connection + "]");
                DBUtils.returnConnection();
            }
        }).print();
    }

    private static void filterFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        env.fromCollection(list)
                .map(x -> x + 1)
                .filter(x -> x > 5)
                .print();
    }

    private static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        DataSource<Integer> data = env.fromCollection(list);
        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        }).print();
        data.map((MapFunction<Integer, Integer>) (value) -> {
            return value + 1;
        }).print();
    }
}

package com.imooc.flink.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author: likaiqing
 * @create: 2019-04-05 12:08
 **/
public class BatchWCJavaApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String input = "file:///Users/likaiqing/space/learn/flink-train-java/test.csv";
        DataSource<String> text = env.readTextFile(input);
        /*
        Java lambda方式需要指定返回类型通过returns方法实现，否则报错org.apache.flink.api.common.functions.InvalidTypesException: The return type of function.... could not be determined automatically, due to type erasure
         */
        /*
        import org.apache.flink.api.common.typeinfo.Types;注意，不要引错
         */
        text.flatMap((FlatMapFunction<String, String>) (value, collect) -> {
            for (String s : value.toLowerCase().split("\\s")) {
                collect.collect(s);
            }
        }).returns(Types.STRING).print();//returns(TypeInformation<OUT> typeInfo)
        text.flatMap((FlatMapFunction<String, String>) (value, collect) -> {
            for (String s : value.toLowerCase().split("\\s")) {
                collect.collect(s);
            }
        }).returns(String.class).print();//returns(Class<OUT> typeClass)
        text.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, collect) -> {
            for (String s : value.toLowerCase().split("\\s")) {
                collect.collect(new Tuple2<>(s, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).print();//returns(TypeInformation<OUT> typeInfo),这是其他两种方式的最根本方法
        text.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, collect) -> {
            for (String s : value.toLowerCase().split("\\s")) {
                collect.collect(new Tuple2<>(s, 1));
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {//returns(TypeHint<OUT> typeHint)
        }).print();//

        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collect) throws Exception {
                for (String s : value.toLowerCase().split("\\s")) {
                    collect.collect(new Tuple2<>(s, 1));
                }
            }
        }).groupBy(0).sum(1).print();
    }
}

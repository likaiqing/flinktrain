package com.imooc.flink.java.source;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author: likaiqing
 * @create: 2019-04-07 15:05
 **/
public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        fromCollection(env);
//        textFile(env);
        csvFile(env);
    }

    private static void csvFile(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///Users/likaiqing/space/learn/flinktrain/people.csv";
        env.readCsvFile(filePath)
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .types(String.class,Integer.class,String.class)
//                .tupleType(Types.TUPLE(Types.STRING, Types.INT, Types.STRING))
//                .pojoType(Person.class, "name", "age")
                .print();
    }

    private static void textFile(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///Users/likaiqing/space/learn/flinktrain/test.csv";
        filePath = "file:///Users/likaiqing/space/learn/flinktrain/result/";
        env.readTextFile(filePath).print();
    }

    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        env.fromCollection(list).print();
    }
}

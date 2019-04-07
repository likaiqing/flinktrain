package com.imooc.flink.java;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author: likaiqing
 * @create: 2019-04-05 12:04
 **/
public class Test {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1, 2, 3)
                .map(i -> Tuple2.of(i, i))    // no information about fields of Tuple2
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();
    }
}

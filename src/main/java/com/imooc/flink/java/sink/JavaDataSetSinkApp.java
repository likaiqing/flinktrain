package com.imooc.flink.java.sink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;
import java.util.List;

/**
 * @author: likaiqing
 * @create: 2019-04-07 18:19
 **/
public class JavaDataSetSinkApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        DataSource<Integer> data = env.fromCollection(list);
        data.writeAsText("file:///Users/likaiqing/space/learn/flinktrain/result", FileSystem.WriteMode.OVERWRITE);

        env.execute("JavaDataSetSinkApp");
    }
}

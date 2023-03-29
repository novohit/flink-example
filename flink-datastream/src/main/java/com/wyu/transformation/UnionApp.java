package com.wyu.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author novo
 * @since 2023-03-29
 */
public class UnionApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source1 = env.readTextFile("data/access.log");
        DataStreamSource<String> source2 = env.readTextFile("data/word.data");
        // union 算子
        // 也可以自己union自己 输出两遍
        source1.union(source2).print();
        env.execute();
    }
}

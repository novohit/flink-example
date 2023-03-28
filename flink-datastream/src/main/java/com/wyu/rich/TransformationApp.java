package com.wyu.rich;

import com.wyu.transformation.model.Log;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author novo
 * @since 2023-03-28
 */
public class TransformationApp {
    public static void main(String[] args) throws Exception {
        // 1. 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        /**
         * map算子 从T类型转到O类型 类型也可以相同
         */
        source.map(new MyRichMapFunction())
                .print();
        // 4. sink
        env.execute();
    }

}

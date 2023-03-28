package com.wyu.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * @author novo
 * @since 2023-03-28
 */
public class ParallelismApp {
    public static void main(String[] args) throws Exception {
        // 1. 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(2); // 可以从上下文设置全局的并行度
        fromCollection(env);
        // 4. sink
        // 5. 执行
        env.execute();
    }

    public static void fromCollection(StreamExecutionEnvironment env) {
        DataStreamSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        System.out.println(source.getParallelism()); // 并行的source
        SingleOutputStreamOperator<Long> filterStream = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 5;
            }
        });
        System.out.println(filterStream.getParallelism());
        filterStream.print();
    }
}

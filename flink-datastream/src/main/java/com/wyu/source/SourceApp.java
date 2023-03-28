package com.wyu.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * @author novo
 * @since 2023-03-27
 */
public class SourceApp {
    public static void main(String[] args) throws Exception {
        /**
         * 用 StreamExecutionEnvironment.addSource(sourceFunction) 将一个 source 关联到你的程序。
         * Flink 自带了许多预先实现的 source functions，
         * 不过你仍然可以通过实现 SourceFunction 接口编写自定义的非并行 source
         */
        // 1. 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // 可以从上下文设置全局的并行度
//        StreamExecutionEnvironment.createLocalEnvironment();
//        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8888);
        // 2. 对接数据源的数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);
        System.out.println(source.getParallelism()); // 这个是非并行的source，所以并行度为1
        // 3. 业务逻辑 transformation
        SingleOutputStreamOperator<String> filterStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !"hello".equals(value);
            }
        }).setParallelism(5); // 这里也可以设置算子的并行度
        System.out.println(filterStream.getParallelism()); // 缺省并行度为机器的core 16
        System.out.println(filterStream.print());
        // 4. sink
        // 5. 执行
        env.execute();
    }

    public static void fromCollection(StreamExecutionEnvironment env) {
        DataStreamSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
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

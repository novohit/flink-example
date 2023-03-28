package com.wyu;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 词频统计
 *
 * @author novo
 * @since 2023-03-27
 */
public class StreamingWordCountApp {

    public static void main(String[] args) throws Exception {
        // 1. 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 对接数据源的数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        // 3. 业务逻辑 transformation
        source.flatMap(new FlatMapFunction<String, String>() {

                    /**
                     *
                     * @param value The input value.
                     * @param out The collector for returning result values.
                     * @throws Exception
                     */
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(",");
                        for (String word : words) {
                            out.collect(word.toLowerCase().trim());
                        }
                    }
                }).filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return StringUtils.isNotBlank(value);
                    }
                }).map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return new Tuple2<>(value, 1L);
                    }
                }).keyBy(0).sum(1) // keyBy(字段索引).sum(对哪个值求和)
                .print();
        // 4. sink
        // 5. 执行
        env.execute();
    }
}

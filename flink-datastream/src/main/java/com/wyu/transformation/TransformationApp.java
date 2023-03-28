package com.wyu.transformation;

import com.wyu.transformation.model.Log;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author novo
 * @since 2023-03-28
 */
public class TransformationApp {
    public static void main(String[] args) throws Exception {
        // 1. 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        /**
         * map算子 从T类型转到O类型 类型也可以相同
         */
        source.map(new MapFunction<String, Log>() {
                    @Override
                    public Log map(String value) throws Exception {
                        String[] split = value.split(",");
                        Log log = Log.builder()
                                .time(Long.parseLong(split[0].trim()))
                                .domain(split[1].trim())
                                .traffic(Double.parseDouble(split[2].trim()))
                                .build();
                        return log;
                    }
                })
                /**
                 * filter算子 只返回满足条件的值
                 */
                .filter(new FilterFunction<Log>() {
                    @Override
                    public boolean filter(Log value) throws Exception {
                        return value.getTraffic() > 10;
                    }
                })
                .keyBy(Log::getDomain)// 分组字段
                //.sum("traffic")
                /**
                 * reduce算子 对KeyedStream 的元素可以两两进行复杂的逻辑操作包括求和 比如A B C，先对A、B操作返回新的B，再对新的B和C进行操作
                 */
                .reduce(new ReduceFunction<Log>() {
                    @Override
                    public Log reduce(Log value1, Log value2) throws Exception {
                        return Log.builder()
                                .time(value2.getTime())
                                .domain(value1.getDomain())
                                .traffic(value1.getTraffic() + value2.getTraffic())
                                .build();
                    }
                })
                .print();
        // flatMap算子将one element生成零个或多个elements. A flatmap function that splits sentences to words:
        // 4. sink
        env.execute();
    }

}

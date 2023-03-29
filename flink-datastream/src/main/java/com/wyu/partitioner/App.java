package com.wyu.partitioner;

import com.wyu.source.LogSource;
import com.wyu.source.MysqlSource;
import com.wyu.source.model.Student;
import com.wyu.transformation.model.Log;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author novo
 * @since 2023-03-29
 */
public class App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Log> source = env.addSource(new LogSource());
        source.map(new MapFunction<Log, Tuple2<String, Log>>() {
            @Override
            public Tuple2<String, Log> map(Log value) throws Exception {
                return Tuple2.of(value.getDomain(), value);
            }
        }).partitionCustom(new CustomPartitioner(), new KeySelector<Tuple2<String, Log>, String>() {
            @Override
            public String getKey(Tuple2<String, Log> value) throws Exception {
                return value.f0;
            }
        }).print();
        // 最终a.com 是1> b.com是2>
        env.execute();
    }
}

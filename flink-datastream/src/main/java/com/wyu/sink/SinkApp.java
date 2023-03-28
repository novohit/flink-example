package com.wyu.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author novo
 * @since 2023-03-28
 */
@Slf4j
public class SinkApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);
        log.error("source: {}", source.getParallelism());
        source.print();
        env.execute();
    }
}

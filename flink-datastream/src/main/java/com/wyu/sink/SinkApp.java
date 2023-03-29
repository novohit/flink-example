package com.wyu.sink;

import com.wyu.source.LogSource;
import com.wyu.source.model.Student;
import com.wyu.transformation.model.Log;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author novo
 * @since 2023-03-28
 */
@Slf4j
public class SinkApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //DataStreamSource<String> source = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Log> mapStream = source.map(new MapFunction<String, Log>() {

            @Override
            public Log map(String value) throws Exception {
                String[] split = value.split(",");
                Log log = Log.builder()
                        .time(Long.valueOf(split[0]))
                        .domain(split[1])
                        .traffic(Double.valueOf(split[2]))
                        .build();
                return log;
            }
        }).keyBy(Log::getDomain).sum("traffic");
        // 将域名访问流量总和写入数据库demo
        log.error("source: {}", source.getParallelism());
        mapStream.print(); // print()实际上是 addSink(printFunction)
        mapStream.addSink(new MySQLSink());
        env.execute();
    }
}

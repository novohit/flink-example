package com.wyu.source;

import com.wyu.transformation.model.Log;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author novo
 * @since 2023-03-28
 */
public class CustomSourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //DataStreamSource<Log> source = env.addSource(new LogSource());
        DataStreamSource<Log> source = env.addSource(new LogParallelSource());
        // 改的是source是并行度不是env的并行度 所以print还是16
        source.setParallelism(2); // SourceFunction是非并行的，所以不能设置并行度，官网上有说
        // 如果要设置一个并行的Source要实现 ParallelSourceFunction 接口或者继承 RichParallelSourceFunction 类编写
        System.out.println(source.getParallelism());
        System.out.println(env.getParallelism());
        source.print();
        env.execute();
    }
}

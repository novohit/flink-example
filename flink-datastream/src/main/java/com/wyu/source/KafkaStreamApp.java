package com.wyu.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author novo
 * @since 2023-03-28
 */
public class KafkaStreamApp {
    public static void main(String[] args) throws Exception {
        // 1. 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(2); // 可以从上下文设置全局的并行度
        test(env);
        // 4. sink
        // 5. 执行
        env.execute();
    }

    public static void test(StreamExecutionEnvironment env) {
        String broker = "plato.kafka:9092";
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(broker)
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        System.out.println(source.getParallelism()); // 也是并行的
        source.print(); // k-v 只会print value
    }
}

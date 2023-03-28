package com.wyu.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.time.Duration;
import java.util.Properties;

/**
 * @author novo
 * @since 2023-03-28
 */
public class KafkaStreamApp {
    public static void main(String[] args) throws Exception {
        /**
         * 以kafka的 input-topic作为源，输出到kafka的output-topic
         * 网站指定1.13可以找到旧版的写法
         * https://nightlies.apache.org/flink/flink-docs-release-1.13/zh/docs/connectors/datastream/kafka/
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String broker = "plato.kafka:9092";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", broker);
        properties.setProperty("group.id", "test");

        // 创建consumer对接kafka源
        FlinkKafkaConsumer<String> myConsumer =
                new FlinkKafkaConsumer<>("input-topic", new SimpleStringSchema(), properties);
        myConsumer.assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)));

        DataStream<String> source = env.addSource(myConsumer);
        System.out.println(source.getParallelism()); // 也是并行的
        source.print(); // k-v 只会print value


        // 创建producer 输出到kafka
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                "output-topic",                  // 目标 topic
                new SimpleStringSchema(),    // 序列化 schema
                properties); // producer 配置

        source.addSink(myProducer);
        env.execute();
    }
}
